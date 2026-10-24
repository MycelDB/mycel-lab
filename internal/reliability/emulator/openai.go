package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
)

const maxRequestBodyBytes = 1 << 20

type OpenAIConfig struct {
	Dimensions       int
	Script           provider.ProtocolEmulatorScript
	ObservationsPath string
}

type Observation struct {
	Time                 time.Time `json:"time"`
	Method               string    `json:"method"`
	Path                 string    `json:"path"`
	ContentLength        int64     `json:"contentLength"`
	Capability           string    `json:"capability"`
	Model                string    `json:"model,omitempty"`
	ResponseModel        string    `json:"responseModel,omitempty"`
	ProviderRequestID    string    `json:"providerRequestId,omitempty"`
	Sequence             int       `json:"sequence"`
	Attempt              int       `json:"attempt"`
	BatchItemCount       int       `json:"batchItemCount"`
	BodySHA256           string    `json:"bodySHA256"`
	InputSHA256          string    `json:"inputSHA256,omitempty"`
	AuthorizationPresent bool      `json:"authorizationPresent"`
	AuthorizationScheme  string    `json:"authorizationScheme,omitempty"`
	Outcome              string    `json:"outcome"`
	Status               int       `json:"status"`
	Canceled             bool      `json:"canceled,omitempty"`
	DurationMilliseconds int64     `json:"durationMilliseconds"`
}

type OpenAIHandler struct {
	config       OpenAIConfig
	mu           sync.Mutex
	sequence     int
	attempts     map[string]int
	observations []Observation
}

func NewOpenAIHandler(config OpenAIConfig) (*OpenAIHandler, error) {
	if config.Dimensions <= 0 || config.Dimensions > 65536 {
		return nil, errors.New("emulator dimensions must be between 1 and 65536")
	}
	if err := provider.ValidateProtocolEmulatorScript(&config.Script); err != nil {
		return nil, fmt.Errorf("validate emulator script: %w", err)
	}
	for _, entry := range config.Script.Entries {
		for _, outcome := range entry.Outcomes {
			if !validProtocolIdentity(outcome.Model, 256) || !validProtocolIdentity(outcome.RequestID, 128) {
				return nil, errors.New("emulator script contains an unsafe response identity")
			}
		}
	}
	if config.ObservationsPath != "" {
		if err := os.MkdirAll(filepath.Dir(config.ObservationsPath), 0o700); err != nil {
			return nil, err
		}
		if err := os.WriteFile(config.ObservationsPath, nil, 0o600); err != nil {
			return nil, err
		}
	}
	return &OpenAIHandler{config: config, attempts: map[string]int{}}, nil
}

func (h *OpenAIHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/healthz" {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"status":"ok"}`))
		return
	}
	if request.URL.Path == "/control/observations" && request.Method == http.MethodGet {
		h.mu.Lock()
		observations := append([]Observation(nil), h.observations...)
		h.mu.Unlock()
		sort.Slice(observations, func(i, j int) bool { return observations[i].Sequence < observations[j].Sequence })
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{"observations": observations})
		return
	}
	if request.Method != http.MethodPost {
		http.NotFound(response, request)
		return
	}
	switch request.URL.Path {
	case "/v1/embeddings":
		h.serveEmbeddings(response, request)
	case "/v1/chat/completions":
		h.serveChat(response, request)
	default:
		http.NotFound(response, request)
	}
}

func (h *OpenAIHandler) serveEmbeddings(response http.ResponseWriter, request *http.Request) {
	started := time.Now()
	limited := io.LimitReader(request.Body, maxRequestBodyBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil || len(body) > maxRequestBodyBytes {
		http.Error(response, "bounded request body required", http.StatusRequestEntityTooLarge)
		return
	}
	bodySum := sha256.Sum256(body)
	observation := Observation{Time: started.UTC(), Method: request.Method, Path: request.URL.Path, ContentLength: int64(len(body)), Capability: "embedding", BodySHA256: hex.EncodeToString(bodySum[:])}
	authorization := strings.TrimSpace(request.Header.Get("Authorization"))
	observation.AuthorizationPresent = authorization != ""
	observation.AuthorizationScheme = safeAuthorizationScheme(authorization)
	var payload struct {
		Model string          `json:"model"`
		Input json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		observation.Outcome = "invalid-request"
		observation.Status = http.StatusBadRequest
		h.finishObservation(&observation, started)
		http.Error(response, "invalid request", http.StatusBadRequest)
		return
	}
	if payload.Model == "" || !validProtocolIdentity(payload.Model, 256) {
		observation.Outcome = "invalid-request"
		observation.Status = http.StatusBadRequest
		h.finishObservation(&observation, started)
		http.Error(response, "invalid model identifier", http.StatusBadRequest)
		return
	}
	inputs, err := decodeEmbeddingInputs(payload.Input)
	if err != nil || len(inputs) == 0 || len(inputs) > 1024 {
		observation.Outcome = "invalid-request"
		observation.Status = http.StatusBadRequest
		h.finishObservation(&observation, started)
		http.Error(response, "invalid input", http.StatusBadRequest)
		return
	}
	observation.Model = payload.Model
	observation.BatchItemCount = len(inputs)
	inputDigest := digestInputs(inputs)
	observation.InputSHA256 = inputDigest
	outcome, attempt, sequence, selectErr := h.selectOutcome(inputDigest)
	observation.Attempt = attempt
	observation.Sequence = sequence
	if selectErr != nil {
		observation.Outcome = "script-rejected"
		observation.Status = http.StatusPreconditionFailed
		h.finishObservation(&observation, started)
		http.Error(response, selectErr.Error(), http.StatusPreconditionFailed)
		return
	}
	observation.Outcome = outcome.Kind
	if outcome.DelayMS > 0 {
		timer := time.NewTimer(time.Duration(outcome.DelayMS) * time.Millisecond)
		select {
		case <-request.Context().Done():
			timer.Stop()
			observation.Outcome = "canceled"
			observation.Canceled = true
			observation.Status = 499
			h.finishObservation(&observation, started)
			return
		case <-timer.C:
		}
	}
	requestID := outcome.RequestID
	if requestID == "" {
		requestID = fmt.Sprintf("emulator-%06d", sequence)
	}
	observation.ProviderRequestID = requestID
	response.Header().Set("X-Request-Id", requestID)
	if outcome.RetryAfterMS > 0 {
		response.Header().Set("Retry-After", strconv.FormatFloat(float64(outcome.RetryAfterMS)/1000, 'f', 3, 64))
	}
	switch outcome.Kind {
	case "http-error":
		observation.Status = outcome.Status
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(outcome.Status)
		_ = json.NewEncoder(response).Encode(map[string]any{"error": map[string]any{"message": "scripted provider error", "type": "emulator_error", "code": "emulator_" + strconv.Itoa(outcome.Status)}})
	case "malformed-json":
		observation.Status = http.StatusOK
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte(`{"data": nope}`))
	case "truncated-json":
		observation.Status = http.StatusOK
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte(`{"data":[`))
	case "refusal":
		observation.Status = http.StatusForbidden
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(response).Encode(map[string]any{"error": map[string]any{"message": "scripted content policy refusal", "type": "content_policy", "code": "content_policy_refusal"}})
	case "wrong-dimensions", "success":
		dimensions := h.config.Dimensions
		if outcome.Kind == "wrong-dimensions" {
			dimensions = outcome.Dimensions
			if dimensions == 0 || dimensions == h.config.Dimensions {
				dimensions = h.config.Dimensions + 1
			}
		}
		model := payload.Model
		if outcome.Model != "" {
			model = outcome.Model
		}
		observation.ResponseModel = model
		data := make([]map[string]any, 0, len(inputs))
		tokens := int64(0)
		for index, input := range inputs {
			data = append(data, map[string]any{"object": "embedding", "index": index, "embedding": TokenBasisV1(input, dimensions)})
			tokens += int64(len(strings.Fields(normalizeInput(input))))
		}
		observation.Status = http.StatusOK
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{"object": "list", "data": data, "model": model, "usage": map[string]any{"prompt_tokens": tokens, "total_tokens": tokens}})
	default:
		observation.Status = http.StatusInternalServerError
		http.Error(response, "unsupported emulator outcome", http.StatusInternalServerError)
	}
	h.finishObservation(&observation, started)
}

func (h *OpenAIHandler) serveChat(response http.ResponseWriter, request *http.Request) {
	started := time.Now()
	limited := io.LimitReader(request.Body, maxRequestBodyBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil || len(body) > maxRequestBodyBytes {
		http.Error(response, "bounded request body required", http.StatusRequestEntityTooLarge)
		return
	}
	bodySum := sha256.Sum256(body)
	observation := Observation{Time: started.UTC(), Method: request.Method, Path: request.URL.Path, ContentLength: int64(len(body)), Capability: "generation", BatchItemCount: 1, BodySHA256: hex.EncodeToString(bodySum[:])}
	authorization := strings.TrimSpace(request.Header.Get("Authorization"))
	observation.AuthorizationPresent = authorization != ""
	observation.AuthorizationScheme = safeAuthorizationScheme(authorization)
	var payload struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Model == "" || !validProtocolIdentity(payload.Model, 256) || len(payload.Messages) == 0 || len(payload.Messages) > 64 {
		observation.Outcome, observation.Status = "invalid-request", http.StatusBadRequest
		h.finishObservation(&observation, started)
		http.Error(response, "invalid request", http.StatusBadRequest)
		return
	}
	parts := make([]string, 0, len(payload.Messages)*2)
	tokens := int64(0)
	for _, message := range payload.Messages {
		var content string
		if err := json.Unmarshal(message.Content, &content); err != nil || strings.TrimSpace(content) == "" || !validProtocolIdentity(message.Role, 32) {
			observation.Outcome, observation.Status = "invalid-request", http.StatusBadRequest
			h.finishObservation(&observation, started)
			http.Error(response, "invalid messages", http.StatusBadRequest)
			return
		}
		parts = append(parts, message.Role, content)
		tokens += int64(len(strings.Fields(normalizeInput(content))))
	}
	observation.Model = payload.Model
	inputDigest := digestInputs(parts)
	observation.InputSHA256 = inputDigest
	outcome, attempt, sequence, selectErr := h.selectOutcome(inputDigest)
	observation.Attempt, observation.Sequence = attempt, sequence
	if selectErr != nil {
		observation.Outcome, observation.Status = "script-rejected", http.StatusPreconditionFailed
		h.finishObservation(&observation, started)
		http.Error(response, selectErr.Error(), http.StatusPreconditionFailed)
		return
	}
	observation.Outcome = outcome.Kind
	if outcome.DelayMS > 0 {
		timer := time.NewTimer(time.Duration(outcome.DelayMS) * time.Millisecond)
		select {
		case <-request.Context().Done():
			timer.Stop()
			observation.Outcome, observation.Canceled, observation.Status = "canceled", true, 499
			h.finishObservation(&observation, started)
			return
		case <-timer.C:
		}
	}
	requestID := outcome.RequestID
	if requestID == "" {
		requestID = fmt.Sprintf("emulator-%06d", sequence)
	}
	// The response stays protocol-realistic, but generation acceptance evidence
	// intentionally excludes provider request IDs.
	response.Header().Set("X-Request-Id", requestID)
	if outcome.RetryAfterMS > 0 {
		response.Header().Set("Retry-After", strconv.FormatFloat(float64(outcome.RetryAfterMS)/1000, 'f', 3, 64))
	}
	switch outcome.Kind {
	case "http-error":
		observation.Status = outcome.Status
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(outcome.Status)
		_ = json.NewEncoder(response).Encode(map[string]any{"error": map[string]any{"message": "scripted provider error", "type": "emulator_error", "code": "emulator_" + strconv.Itoa(outcome.Status)}})
	case "malformed-json":
		observation.Status = http.StatusOK
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"choices": nope}`))
	case "truncated-json":
		observation.Status = http.StatusOK
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"choices":[`))
	case "refusal":
		observation.Status = http.StatusForbidden
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(response).Encode(map[string]any{"error": map[string]any{"message": "scripted content policy refusal", "type": "content_policy", "code": "content_policy_refusal"}})
	case "success":
		model := payload.Model
		if outcome.Model != "" {
			model = outcome.Model
		}
		observation.ResponseModel, observation.Status = model, http.StatusOK
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"id": requestID, "object": "chat.completion", "model": model,
			"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": "procedure-inference-emulator-result"}, "finish_reason": "stop"}},
			"usage":   map[string]any{"prompt_tokens": tokens, "completion_tokens": 3, "total_tokens": tokens + 3},
		})
	default:
		observation.Status = http.StatusInternalServerError
		http.Error(response, "unsupported emulator outcome", http.StatusInternalServerError)
	}
	h.finishObservation(&observation, started)
}

func (h *OpenAIHandler) selectOutcome(inputDigest string) (provider.ProtocolEmulatorOutcome, int, int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sequence >= h.config.Script.MaxRequests {
		return provider.ProtocolEmulatorOutcome{}, 0, h.sequence, errors.New("emulator request ceiling reached")
	}
	h.sequence++
	sequence := h.sequence
	for _, entry := range h.config.Script.Entries {
		if entry.InputSHA256 != inputDigest {
			continue
		}
		attempt := h.attempts[inputDigest] + 1
		h.attempts[inputDigest] = attempt
		if attempt > len(entry.Outcomes) {
			return provider.ProtocolEmulatorOutcome{}, attempt, sequence, errors.New("emulator outcome sequence exhausted")
		}
		return entry.Outcomes[attempt-1], attempt, sequence, nil
	}
	attempt := h.attempts[inputDigest] + 1
	h.attempts[inputDigest] = attempt
	if h.config.Script.RequireMatch {
		return provider.ProtocolEmulatorOutcome{}, attempt, sequence, errors.New("emulator script has no matching input")
	}
	return provider.ProtocolEmulatorOutcome{Kind: "success"}, attempt, sequence, nil
}

func (h *OpenAIHandler) finishObservation(observation *Observation, started time.Time) {
	observation.DurationMilliseconds = time.Since(started).Milliseconds()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.observations = append(h.observations, *observation)
	path := h.config.ObservationsPath
	if path == "" {
		return
	}
	raw, err := json.Marshal(observation)
	if err != nil {
		return
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return
	}
	_, _ = file.Write(append(raw, '\n'))
	_ = file.Close()
}

func decodeEmbeddingInputs(raw json.RawMessage) ([]string, error) {
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		if strings.TrimSpace(single) == "" {
			return nil, errors.New("embedding input is empty")
		}
		return []string{single}, nil
	}
	var batch []string
	if err := json.Unmarshal(raw, &batch); err != nil {
		return nil, err
	}
	for _, input := range batch {
		if strings.TrimSpace(input) == "" {
			return nil, errors.New("embedding batch contains empty input")
		}
	}
	return batch, nil
}

func safeAuthorizationScheme(value string) string {
	if value == "" {
		return ""
	}
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return "Other"
	}
	switch strings.ToLower(fields[0]) {
	case "bearer":
		return "Bearer"
	case "basic":
		return "Basic"
	default:
		return "Other"
	}
}

func validProtocolIdentity(value string, maximum int) bool {
	if value == "" {
		return true
	}
	if len(value) > maximum {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' {
			continue
		}
		switch character {
		case '.', '_', ':', '/', '-':
			continue
		default:
			return false
		}
	}
	return true
}

func digestInputs(inputs []string) string {
	hasher := sha256.New()
	for index, input := range inputs {
		if index > 0 {
			_, _ = hasher.Write([]byte{0})
		}
		_, _ = hasher.Write([]byte(normalizeInput(input)))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func LoadProtocolScript(raw []byte) (provider.ProtocolEmulatorScript, error) {
	var script provider.ProtocolEmulatorScript
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&script); err != nil {
		return provider.ProtocolEmulatorScript{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return provider.ProtocolEmulatorScript{}, errors.New("emulator script contains trailing JSON")
		}
		return provider.ProtocolEmulatorScript{}, fmt.Errorf("decode trailing emulator script data: %w", err)
	}
	return script, nil
}

func Serve(ctx context.Context, listenerAddress string, handler http.Handler) error {
	server := &http.Server{Addr: listenerAddress, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 35 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
