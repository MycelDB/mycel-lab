package provider

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRedactorSanitizesStructuredTextAndSubprocessArguments(t *testing.T) {
	const opaque = "z9-Qp7-unrecognized-credential"
	redactor := NewRedactor([]string{opaque})
	input := map[string]any{
		"Authorization": "Bearer abcdefghijklmnop",
		"nested": map[string]any{
			"api_key": "sk-1234567890abcdefghijklmnop",
			"note":    "prefix " + opaque + " suffix",
		},
		"args":        []any{"mycel", "--password", "visible-password", "--space", "demo"},
		"certificate": "-----BEGIN PRIVATE KEY-----\nmaterial\n-----END PRIVATE KEY-----",
		"log":         `password: "multi word credential"`,
	}
	clean, err := redactor.Sanitize(input)
	if err != nil {
		t.Fatalf("Sanitize() error = %v", err)
	}
	raw, err := json.Marshal(clean)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, prohibited := range []string{opaque, "visible-password", "abcdefghijklmnop", "material", "multi word credential"} {
		if strings.Contains(text, prohibited) {
			t.Fatalf("sanitized value contains %q: %s", prohibited, text)
		}
	}
	if count := strings.Count(text, Redacted); count < 4 {
		t.Fatalf("redaction marker count = %d: %s", count, text)
	}
}

func TestScannerFindsConfiguredAndPatternCredentialsWithoutPersistingValues(t *testing.T) {
	const opaque = "opaque-not-shaped-like-a-token"
	root := t.TempDir()
	content := "safe prefix " + opaque + "\nAuthorization: Bearer abcdefghijklmno\n"
	if err := os.WriteFile(filepath.Join(root, "unsafe.log"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(NewRedactor([]string{opaque}), DefaultScanLimits())
	if err != nil {
		t.Fatal(err)
	}
	report, err := scanner.Scan(root)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if report.OK || len(report.Findings) < 2 {
		t.Fatalf("report = %#v", report)
	}
	raw, _ := json.Marshal(report)
	if strings.Contains(string(raw), opaque) || strings.Contains(string(raw), "abcdefghijklmno") {
		t.Fatalf("finding report leaked a credential: %s", raw)
	}
}

func TestScannerAcceptsSanitizedArtifacts(t *testing.T) {
	const secret = "secret-with-no-standard-shape"
	redactor := NewRedactor([]string{secret})
	root := t.TempDir()
	clean := redactor.SanitizeText("credential=" + secret + " authorization=Bearer abcdefghijklmnop")
	if err := os.WriteFile(filepath.Join(root, "safe.log"), []byte(clean), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(redactor, DefaultScanLimits())
	if err != nil {
		t.Fatal(err)
	}
	report, err := scanner.Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK || len(report.Findings) != 0 {
		t.Fatalf("report = %#v, content=%q", report, clean)
	}
}

func TestScannerFailsClosedOnBounds(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "large"), []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(NewRedactor(nil), ScanLimits{MaxFiles: 1, MaxFileBytes: 4, MaxTotalBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scanner.Scan(root); err == nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("Scan() error = %v", err)
	}
}
