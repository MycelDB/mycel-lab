package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MycelDB/mycel-lab/internal/reliability/provider"
)

func TestOpenAIEmulatorSuccessRecordsSanitizedEnvelope(t *testing.T) {
	handler := mustOpenAIHandler(t, provider.ProtocolEmulatorScript{Version: "v1", MaxRequests: 4})
	server := httptest.NewServer(handler)
	defer server.Close()
	body := `{"model":"embedding-v1","input":"axis-3"}`
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/embeddings", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer do-not-record-this-value")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var payload struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Data) != 1 || len(payload.Data[0].Embedding) != 8 || payload.Data[0].Embedding[3] != 1 {
		t.Fatalf("payload = %+v", payload)
	}
	second, err := http.Post(server.URL+"/v1/embeddings", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	second.Body.Close()
	observations := handler.observationSnapshot()
	if len(observations) != 2 || observations[0].BatchItemCount != 1 || !observations[0].AuthorizationPresent || observations[0].AuthorizationScheme != "Bearer" || observations[0].BodySHA256 == "" || observations[0].InputSHA256 == "" {
		t.Fatalf("observations = %+v", observations)
	}
	if observations[0].Attempt != 1 || observations[1].Attempt != 2 || observations[0].ResponseModel != "embedding-v1" || observations[0].ProviderRequestID == "" {
		t.Fatalf("repeated default observations = %+v", observations)
	}
	raw, _ := json.Marshal(observations)
	if bytes.Contains(raw, []byte("do-not-record-this-value")) || bytes.Contains(raw, []byte("axis-3")) {
		t.Fatalf("observation leaked header or body: %s", raw)
	}
}

func TestOpenAIEmulatorChatSuccessRecordsOnlySanitizedDigests(t *testing.T) {
	handler := mustOpenAIHandler(t, provider.ProtocolEmulatorScript{Version: "v1", MaxRequests: 2})
	server := httptest.NewServer(handler)
	defer server.Close()
	body := `{"model":"procedure-generation-v1","messages":[{"role":"system","content":"private procedure prompt"},{"role":"user","content":"private rendered graph content"}]}`
	response, err := http.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var payload struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens, CompletionTokens, TotalTokens int64
		} `json:"usage"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(payload.Choices) != 1 || payload.Choices[0].Message.Content != "procedure-inference-emulator-result" {
		t.Fatalf("status=%d payload=%+v", response.StatusCode, payload)
	}
	observations := handler.observationSnapshot()
	if len(observations) != 1 || observations[0].Capability != "generation" || observations[0].InputSHA256 == "" || observations[0].BatchItemCount != 1 || observations[0].ProviderRequestID != "" {
		t.Fatalf("observations=%+v", observations)
	}
	raw, _ := json.Marshal(observations)
	if bytes.Contains(raw, []byte("private procedure prompt")) || bytes.Contains(raw, []byte("private rendered graph content")) || bytes.Contains(raw, []byte("procedure-inference-emulator-result")) || bytes.Contains(raw, []byte("emulator-000001")) {
		t.Fatalf("chat observation leaked content: %s", raw)
	}
}

func TestOpenAIEmulatorScriptedFailuresAndSequenceCeiling(t *testing.T) {
	digest := digestInputs([]string{"axis-4"})
	handler := mustOpenAIHandler(t, provider.ProtocolEmulatorScript{Version: "v1", RequireMatch: true, MaxRequests: 3, Entries: []provider.ProtocolEmulatorScriptEntry{{InputSHA256: digest, Outcomes: []provider.ProtocolEmulatorOutcome{{Kind: "http-error", Status: 429, RetryAfterMS: 250}, {Kind: "wrong-dimensions", Dimensions: 3}, {Kind: "malformed-json"}}}}})
	server := httptest.NewServer(handler)
	defer server.Close()
	for index, wantStatus := range []int{429, 200, 200} {
		response, err := http.Post(server.URL+"/v1/embeddings", "application/json", strings.NewReader(`{"model":"embedding-v1","input":"axis-4"}`))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != wantStatus {
			t.Fatalf("attempt %d status = %d body=%s", index+1, response.StatusCode, body)
		}
		if index == 0 && response.Header.Get("Retry-After") != "0.250" {
			t.Fatalf("Retry-After = %q", response.Header.Get("Retry-After"))
		}
		if response.Header.Get("X-Request-Id") == "" {
			t.Fatalf("attempt %d missing safe request ID", index+1)
		}
		if index == 1 {
			var payload struct {
				Data []struct {
					Embedding []float64 `json:"embedding"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &payload); err != nil || len(payload.Data) != 1 || len(payload.Data[0].Embedding) != 3 {
				t.Fatalf("wrong dimensions payload = %s err=%v", body, err)
			}
		}
		if index == 2 && json.Valid(body) {
			t.Fatalf("malformed outcome returned valid JSON: %s", body)
		}
	}
	response, err := http.Post(server.URL+"/v1/embeddings", "application/json", strings.NewReader(`{"model":"embedding-v1","input":"axis-4"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("ceiling status = %d", response.StatusCode)
	}
}

func TestOpenAIEmulatorRejectedInputsAdvanceAttempts(t *testing.T) {
	matchedDigest := digestInputs([]string{"matched"})
	handler := mustOpenAIHandler(t, provider.ProtocolEmulatorScript{Version: "v1", RequireMatch: true, MaxRequests: 5, Entries: []provider.ProtocolEmulatorScriptEntry{{InputSHA256: matchedDigest, Outcomes: []provider.ProtocolEmulatorOutcome{{Kind: "success"}}}}})
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, input := range []string{"unmatched", "unmatched", "matched", "matched", "matched"} {
		response, err := http.Post(server.URL+"/v1/embeddings", "application/json", strings.NewReader(`{"model":"embedding-v1","input":"`+input+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	}
	observations := handler.observationSnapshot()
	if len(observations) != 5 {
		t.Fatalf("observations = %+v", observations)
	}
	if observations[0].Attempt != 1 || observations[1].Attempt != 2 || observations[2].Attempt != 1 || observations[3].Attempt != 2 || observations[4].Attempt != 3 {
		t.Fatalf("rejected attempts did not advance: %+v", observations)
	}
}

func TestOpenAIEmulatorSanitizesUnrecognizedAuthorizationAndUnsafeModel(t *testing.T) {
	handler := mustOpenAIHandler(t, provider.ProtocolEmulatorScript{Version: "v1", MaxRequests: 2})
	server := httptest.NewServer(handler)
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/embeddings", strings.NewReader(`{"model":"embedding-v1","input":"axis-1"}`))
	request.Header.Set("Authorization", "opaque-secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	unsafeModel := strings.Repeat("sensitive text ", 30)
	response, err = http.Post(server.URL+"/v1/embeddings", "application/json", strings.NewReader(`{"model":`+strconv.Quote(unsafeModel)+`,"input":"axis-2"}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unsafe model status = %d", response.StatusCode)
	}
	observations := handler.observationSnapshot()
	if len(observations) != 2 || observations[0].AuthorizationScheme != "Other" || observations[1].Model != "" {
		t.Fatalf("observations = %+v", observations)
	}
	raw, _ := json.Marshal(observations)
	if bytes.Contains(raw, []byte("opaque-secret")) || bytes.Contains(raw, []byte("sensitive text")) {
		t.Fatalf("observation leaked request-controlled values: %s", raw)
	}
}

func TestOpenAIEmulatorRejectsUnsafeScriptedResponseIdentity(t *testing.T) {
	_, err := NewOpenAIHandler(OpenAIConfig{Dimensions: 8, Script: provider.ProtocolEmulatorScript{Version: "v1", MaxRequests: 1, Entries: []provider.ProtocolEmulatorScriptEntry{{InputSHA256: strings.Repeat("a", 64), Outcomes: []provider.ProtocolEmulatorOutcome{{Kind: "success", Model: "sensitive response text"}}}}}})
	if err == nil {
		t.Fatal("NewOpenAIHandler() accepted unsafe scripted response identity")
	}
}

func TestLoadProtocolScriptRejectsTrailingJSON(t *testing.T) {
	if _, err := LoadProtocolScript([]byte(`{"version":"v1","max_requests":1} {}`)); err == nil {
		t.Fatal("LoadProtocolScript() accepted trailing JSON")
	}
}

func TestOpenAIEmulatorDelayHonorsCancellation(t *testing.T) {
	digest := digestInputs([]string{"axis-5"})
	handler := mustOpenAIHandler(t, provider.ProtocolEmulatorScript{Version: "v1", RequireMatch: true, MaxRequests: 1, Entries: []provider.ProtocolEmulatorScriptEntry{{InputSHA256: digest, Outcomes: []provider.ProtocolEmulatorOutcome{{Kind: "success", DelayMS: 5000}}}}})
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/embeddings", strings.NewReader(`{"model":"embedding-v1","input":"axis-5"}`))
	_, err := http.DefaultClient.Do(request)
	if err == nil {
		t.Fatal("delayed request was not canceled")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		observations := handler.observationSnapshot()
		if len(observations) == 1 {
			if !observations[0].Canceled || observations[0].Status != 499 {
				t.Fatalf("observation = %+v", observations[0])
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("canceled observation was not recorded")
}

func mustOpenAIHandler(t *testing.T, script provider.ProtocolEmulatorScript) *OpenAIHandler {
	t.Helper()
	handler, err := NewOpenAIHandler(OpenAIConfig{Dimensions: 8, Script: script, ObservationsPath: t.TempDir() + "/observations.jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func (h *OpenAIHandler) observationSnapshot() []Observation {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Observation(nil), h.observations...)
}
