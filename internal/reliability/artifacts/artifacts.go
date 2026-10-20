package artifacts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const maxArtifactWriteBytes = 64 << 20

type Sanitizer interface {
	Sanitize(value any) (any, error)
	SanitizeText(value string) string
}

type Sink struct {
	root      string
	mu        sync.Mutex
	sanitizer Sanitizer
}

type Event struct {
	Time      time.Time `json:"time"`
	Type      string    `json:"type"`
	PhaseName string    `json:"phaseName,omitempty"`
	ActorID   string    `json:"actorId,omitempty"`
	Payload   any       `json:"payload,omitempty"`
}

func NewSink(root string) (*Sink, error) {
	return NewSanitizedSink(root, nil)
}

func NewSanitizedSink(root string, sanitizer Sanitizer) (*Sink, error) {
	if root == "" {
		root = filepath.Join("artifacts", "runs", time.Now().UTC().Format("20060102-150405"))
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	return &Sink{root: root, sanitizer: sanitizer}, nil
}

func (s *Sink) Root() string { return s.root }

func (s *Sink) Sanitizer() Sanitizer { return s.sanitizer }

func (s *Sink) Sanitize(value any) (any, error) {
	if s.sanitizer == nil {
		return value, nil
	}
	return s.sanitizer.Sanitize(value)
}

func (s *Sink) WriteJSON(name string, value any) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path, err := s.safePath(name)
	if err != nil {
		return "", err
	}
	if err := s.preparePath(path); err != nil {
		return "", err
	}
	clean, err := s.Sanitize(value)
	if err != nil {
		return "", err
	}
	data, err := json.MarshalIndent(clean, "", "  ")
	if err != nil {
		return "", err
	}
	data = append(data, '\n')
	if len(data) > maxArtifactWriteBytes {
		return "", fmt.Errorf("artifact %q exceeds %d-byte write limit", name, maxArtifactWriteBytes)
	}
	return path, os.WriteFile(path, data, 0o600)
}

func (s *Sink) WriteText(name string, content string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path, err := s.safePath(name)
	if err != nil {
		return "", err
	}
	if err := s.preparePath(path); err != nil {
		return "", err
	}
	if len(content) > maxArtifactWriteBytes {
		return "", fmt.Errorf("artifact %q exceeds %d-byte write limit", name, maxArtifactWriteBytes)
	}
	if s.sanitizer != nil {
		content = s.sanitizer.SanitizeText(content)
	}
	return path, os.WriteFile(path, []byte(content), 0o600)
}

func (s *Sink) AppendJSONL(name string, value any) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path, err := s.safePath(name)
	if err != nil {
		return "", err
	}
	if err := s.preparePath(path); err != nil {
		return "", err
	}
	clean, err := s.Sanitize(value)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(clean)
	if err != nil {
		return "", err
	}
	data = append(data, '\n')
	if len(data) > maxArtifactWriteBytes {
		return "", fmt.Errorf("artifact record %q exceeds %d-byte write limit", name, maxArtifactWriteBytes)
	}
	if info, err := os.Stat(path); err == nil && info.Size() > int64(maxArtifactWriteBytes-len(data)) {
		return "", fmt.Errorf("artifact %q exceeds %d-byte cumulative write limit", name, maxArtifactWriteBytes)
	} else if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return "", err
	}
	return path, nil
}

func (s *Sink) safePath(name string) (string, error) {
	clean := filepath.Clean(name)
	if name == "" || clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("artifact path %q must be a relative path within the run root", name)
	}
	return filepath.Join(s.root, clean), nil
}

func (s *Sink) preparePath(path string) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	resolvedRoot, err := filepath.EvalSymlinks(s.root)
	if err != nil {
		return err
	}
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(resolvedRoot, resolvedParent)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("artifact parent %q resolves outside run root", parent)
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("artifact path %q is a symlink", path)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func EventNow(eventType, phaseName, actorID string, payload any) Event {
	return Event{Time: time.Now().UTC(), Type: eventType, PhaseName: phaseName, ActorID: actorID, Payload: payload}
}

func RunRoot(base, runID string) string {
	if base == "" {
		base = filepath.Join("artifacts", "runs")
	}
	if runID == "" {
		runID = time.Now().UTC().Format("20060102-150405")
	}
	return filepath.Join(base, runID)
}

func RequireEmptyOrCreate(path string) error {
	if path == "" {
		return fmt.Errorf("path is required")
	}
	return os.MkdirAll(path, 0o700)
}
