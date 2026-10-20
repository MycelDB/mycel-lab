package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const Redacted = "[REDACTED]"

var sensitiveKeys = map[string]struct{}{
	"password": {}, "newpassword": {}, "oldpassword": {},
	"secret": {}, "secretvalue": {}, "clientsecret": {}, "credential": {}, "credentialvalue": {},
	"apikey": {}, "authorization": {}, "proxyauthorization": {}, "bearer": {},
	"cookie": {}, "setcookie": {}, "accesstoken": {}, "refreshtoken": {},
	"authtoken": {}, "tlscertificate": {}, "tlsprivatekey": {}, "privatekey": {},
}

type textDetector struct {
	category string
	pattern  *regexp.Regexp
}

type Redactor struct {
	secrets   []string
	detectors []textDetector
}

func NewRedactor(secrets []string) *Redactor {
	unique := map[string]struct{}{}
	for _, secret := range secrets {
		if secret != "" {
			unique[secret] = struct{}{}
		}
	}
	values := make([]string, 0, len(unique))
	for secret := range unique {
		values = append(values, secret)
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	return &Redactor{secrets: values, detectors: defaultTextDetectors()}
}

func defaultTextDetectors() []textDetector {
	return []textDetector{
		{category: "bearer-token", pattern: regexp.MustCompile(`(?i)\bbearer[ \t]+[a-z0-9._~+/=-]{8,}`)},
		{category: "api-key", pattern: regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{16,}\b`)},
		{category: "pem-material", pattern: regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]+-----.*?-----END [A-Z0-9 ]+-----`)},
		{category: "credential-assignment", pattern: regexp.MustCompile(`(?i)(password|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|cookie|set-cookie)[ \t]*[:=][ \t]*["'][^"'\r\n]+["']`)},
		{category: "credential-assignment", pattern: regexp.MustCompile(`(?i)(password|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|cookie|set-cookie)[ \t]*[:=][ \t]*[^\s,"']{3,}`)},
	}
}

type sanitizeState struct {
	nodes       int
	stringBytes int64
}

func (r *Redactor) Sanitize(value any) (any, error) {
	return r.sanitize(value, 0, "", &sanitizeState{})
}

func (r *Redactor) sanitize(value any, depth int, parentKey string, state *sanitizeState) (any, error) {
	if depth > 32 {
		return nil, errors.New("redaction input exceeds maximum depth 32")
	}
	state.nodes++
	if state.nodes > 100000 {
		return nil, errors.New("redaction input exceeds maximum node count 100000")
	}
	switch typed := value.(type) {
	case nil, bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return typed, nil
	case string:
		state.stringBytes += int64(len(typed))
		if len(typed) > 8<<20 || state.stringBytes > 16<<20 {
			return nil, errors.New("redaction input exceeds string byte limits")
		}
		if isSensitiveKey(parentKey) {
			return Redacted, nil
		}
		return r.SanitizeText(typed), nil
	case []byte:
		state.stringBytes += int64(len(typed))
		if len(typed) > 8<<20 || state.stringBytes > 16<<20 {
			return nil, errors.New("redaction input exceeds byte limits")
		}
		if isSensitiveKey(parentKey) {
			return []byte(Redacted), nil
		}
		return []byte(r.SanitizeText(string(typed))), nil
	case []string:
		values := make([]any, len(typed))
		for i, item := range typed {
			values[i] = item
		}
		return r.sanitizeSlice(values, depth, state)
	case []any:
		return r.sanitizeSlice(typed, depth, state)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			state.stringBytes += int64(len(key))
			if len(key) > 64<<10 || state.stringBytes > 16<<20 {
				return nil, errors.New("redaction input exceeds map-key byte limits")
			}
			cleanKey := r.SanitizeText(key)
			if cleanKey != key {
				cleanKey = "[REDACTED-KEY]"
			}
			if _, exists := out[cleanKey]; exists {
				return nil, errors.New("redaction produced a duplicate map key")
			}
			if isSensitiveKey(key) {
				out[cleanKey] = Redacted
				continue
			}
			clean, err := r.sanitize(item, depth+1, key, state)
			if err != nil {
				return nil, err
			}
			out[cleanKey] = clean
		}
		return out, nil
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		if len(raw) > 16<<20 {
			return nil, errors.New("redaction input exceeds encoded byte limit")
		}
		var generic any
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&generic); err != nil {
			return nil, err
		}
		return r.sanitize(generic, depth+1, parentKey, state)
	}
}

func (r *Redactor) sanitizeSlice(values []any, depth int, state *sanitizeState) ([]any, error) {
	out := make([]any, len(values))
	redactNext := false
	for i, item := range values {
		if redactNext {
			out[i] = Redacted
			redactNext = false
			continue
		}
		if text, ok := item.(string); ok && isSensitiveFlag(text) {
			out[i] = text
			redactNext = true
			continue
		}
		clean, err := r.sanitize(item, depth+1, "", state)
		if err != nil {
			return nil, err
		}
		out[i] = clean
	}
	return out, nil
}

func (r *Redactor) SanitizeText(text string) string {
	for _, secret := range r.secrets {
		text = strings.ReplaceAll(text, secret, Redacted)
	}
	for _, detector := range r.detectors {
		text = detector.pattern.ReplaceAllStringFunc(text, func(match string) string {
			if detector.category != "credential-assignment" {
				return Redacted
			}
			separator := strings.IndexAny(match, ":=")
			if separator < 0 {
				return Redacted
			}
			return match[:separator+1] + Redacted
		})
	}
	return text
}

func isSensitiveKey(key string) bool {
	var normalized strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(key)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			normalized.WriteRune(r)
		}
	}
	_, ok := sensitiveKeys[normalized.String()]
	return ok
}

func isSensitiveFlag(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.TrimLeft(value, "-")
	value = strings.ReplaceAll(value, "_", "-")
	switch value {
	case "password", "new-password", "old-password", "secret", "secret-value", "api-key", "access-token", "refresh-token", "authorization", "tls-key", "private-key":
		return true
	default:
		return false
	}
}

type ScanLimits struct {
	MaxFiles      int   `json:"maxFiles"`
	MaxFileBytes  int64 `json:"maxFileBytes"`
	MaxTotalBytes int64 `json:"maxTotalBytes"`
}

func DefaultScanLimits() ScanLimits {
	return ScanLimits{MaxFiles: 10000, MaxFileBytes: 64 << 20, MaxTotalBytes: 512 << 20}
}

type Finding struct {
	Path     string `json:"path"`
	Category string `json:"category"`
	Start    int    `json:"start"`
	End      int    `json:"end"`
}

type ScanReport struct {
	SchemaVersion string    `json:"schemaVersion"`
	FilesScanned  int       `json:"filesScanned"`
	BytesScanned  int64     `json:"bytesScanned"`
	Findings      []Finding `json:"findings"`
	OK            bool      `json:"ok"`
}

type Scanner struct {
	redactor *Redactor
	limits   ScanLimits
}

func NewScanner(redactor *Redactor, limits ScanLimits) (*Scanner, error) {
	if redactor == nil {
		redactor = NewRedactor(nil)
	}
	if limits.MaxFiles <= 0 || limits.MaxFileBytes <= 0 || limits.MaxTotalBytes <= 0 || limits.MaxFileBytes > limits.MaxTotalBytes {
		return nil, errors.New("redaction scan limits must be positive and maxFileBytes must not exceed maxTotalBytes")
	}
	return &Scanner{redactor: redactor, limits: limits}, nil
}

func (s *Scanner) Scan(root string) (ScanReport, error) {
	report := ScanReport{SchemaVersion: "reliability.mycel.io/redaction-scan/v1alpha1"}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("redaction scan refuses symlink %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if filepath.ToSlash(relative) == "redaction-scan.json" {
			return nil
		}
		if report.FilesScanned >= s.limits.MaxFiles {
			return errors.New("redaction scan file limit exceeded")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > s.limits.MaxFileBytes {
			return fmt.Errorf("redaction scan file %s exceeds byte limit", relative)
		}
		if report.BytesScanned > s.limits.MaxTotalBytes-info.Size() {
			return errors.New("redaction scan total byte limit exceeded")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		report.FilesScanned++
		report.BytesScanned += int64(len(data))
		report.Findings = append(report.Findings, s.scanBytes(filepath.ToSlash(relative), data)...)
		return nil
	})
	if err != nil {
		return report, err
	}
	sort.Slice(report.Findings, func(i, j int) bool {
		if report.Findings[i].Path == report.Findings[j].Path {
			if report.Findings[i].Start == report.Findings[j].Start {
				return report.Findings[i].Category < report.Findings[j].Category
			}
			return report.Findings[i].Start < report.Findings[j].Start
		}
		return report.Findings[i].Path < report.Findings[j].Path
	})
	report.OK = len(report.Findings) == 0
	return report, nil
}

func (s *Scanner) scanBytes(path string, data []byte) []Finding {
	var findings []Finding
	for _, secret := range s.redactor.secrets {
		if secret == "" {
			continue
		}
		secretBytes := []byte(secret)
		for offset := 0; offset <= len(data)-len(secretBytes); {
			index := bytes.Index(data[offset:], secretBytes)
			if index < 0 {
				break
			}
			start := offset + index
			findings = append(findings, Finding{Path: path, Category: "configured-secret", Start: start, End: start + len(secretBytes)})
			offset = start + len(secretBytes)
		}
	}
	text := string(data)
	for _, detector := range s.redactor.detectors {
		for _, location := range detector.pattern.FindAllStringIndex(text, -1) {
			match := text[location[0]:location[1]]
			if strings.Contains(match, Redacted) {
				continue
			}
			findings = append(findings, Finding{Path: path, Category: detector.category, Start: location[0], End: location[1]})
		}
	}
	if !utf8.Valid(data) {
		// Binary files are still scanned for configured byte strings above; no
		// text-pattern finding is added for invalid UTF-8.
		filtered := findings[:0]
		for _, finding := range findings {
			if finding.Category == "configured-secret" {
				filtered = append(filtered, finding)
			}
		}
		findings = filtered
	}
	return findings
}
