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
		opaque:          "safe-key-value",
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

func TestRedactorSanitizesCompoundCredentialEnvironmentAssignments(t *testing.T) {
	redactor := NewRedactor(nil)
	input := strings.Join([]string{
		`AWS_SECRET_ACCESS_KEY: "fixture-access-secret"`,
		`MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=fixture-backend-token`,
		`MYCELD_ENCRYPTION_STATIC_KEY_B64: fixture-encryption-key`,
		`"AWS_SECRET_ACCESS_KEY": "quoted-access-secret"`,
		`'MYCELD_ENCRYPTION_STATIC_KEY_B64': 'quoted-encryption-key'`,
		`MYCELD_BOOTSTRAP_ADMIN_PASSWORD=x`,
		`SAFE_TOKEN_COUNT: 3`,
	}, "\n")
	clean := redactor.SanitizeText(input)
	for _, prohibited := range []string{"fixture-access-secret", "fixture-backend-token", "fixture-encryption-key", "quoted-access-secret", "quoted-encryption-key", "PASSWORD=x"} {
		if strings.Contains(clean, prohibited) {
			t.Fatalf("sanitized compound environment assignment contains %q: %s", prohibited, clean)
		}
	}
	if !strings.Contains(clean, "SAFE_TOKEN_COUNT: 3") || strings.Count(clean, Redacted) != 6 {
		t.Fatalf("unexpected compound environment sanitization: %s", clean)
	}

	structured, err := redactor.Sanitize(map[string]any{
		"AWS_SECRET_ACCESS_KEY":             "fixture-access-secret",
		"MYCELD_CLUSTER_BACKEND_AUTH_TOKEN": "fixture-backend-token",
		"MYCELD_ENCRYPTION_STATIC_KEY_B64":  "fixture-encryption-key",
		"MYCELD_BOOTSTRAP_ADMIN_PASSWORD":   "x",
		"SAFE_TOKEN_COUNT":                  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(structured)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "fixture-") || !strings.Contains(string(raw), `"SAFE_TOKEN_COUNT":3`) {
		t.Fatalf("unexpected structured compound environment sanitization: %s", raw)
	}
}

func TestScannerFindsCompoundCredentialEnvironmentAssignments(t *testing.T) {
	root := t.TempDir()
	content := strings.Join([]string{
		"AWS_SECRET_ACCESS_KEY=fixture-access-secret",
		"MYCELD_CLUSTER_BACKEND_AUTH_TOKEN: fixture-backend-token",
		"MYCELD_ENCRYPTION_STATIC_KEY_B64=fixture-encryption-key",
		`"AWS_SECRET_ACCESS_KEY": "quoted-access-secret"`,
		`'MYCELD_ENCRYPTION_STATIC_KEY_B64': 'quoted-encryption-key'`,
		"MYCELD_BOOTSTRAP_ADMIN_PASSWORD=x",
	}, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(root, "compose.yaml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(NewRedactor(nil), DefaultScanLimits())
	if err != nil {
		t.Fatal(err)
	}
	report, err := scanner.Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if report.OK || len(report.Findings) != 6 {
		t.Fatalf("report=%#v", report)
	}
}

func TestRedactorRejectsConfiguredSecretKeyCollisions(t *testing.T) {
	redactor := NewRedactor([]string{"first-secret", "second-secret"})
	_, err := redactor.Sanitize(map[string]any{"first-secret": 1, "second-secret": 2})
	if err == nil || !strings.Contains(err.Error(), "duplicate map key") {
		t.Fatalf("Sanitize() error = %v", err)
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
