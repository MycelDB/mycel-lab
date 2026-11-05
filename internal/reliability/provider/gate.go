package provider

// ValidEmulatorGate accepts finite synthetic control names, never paths or URLs.
func ValidEmulatorGate(name string) bool {
	if len(name) == 0 || len(name) > 64 || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for _, c := range name {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}
