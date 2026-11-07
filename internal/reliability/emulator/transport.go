package emulator

import "net/http"

// Publish the finite evidence before EOF becomes visible to the caller. This is
// a transport event, not an HTTP 503 response; the status is a synthetic marker.
func dropProviderConnection(w http.ResponseWriter, observation *Observation, record func()) {
	if hijacker, ok := w.(http.Hijacker); ok {
		connection, _, err := hijacker.Hijack()
		if err == nil {
			observation.Status = http.StatusServiceUnavailable // synthetic envelope; no HTTP response was sent
			record()
			_ = connection.Close()
			return
		}
	}
	observation.Outcome, observation.Status = "unsupported-transport", http.StatusNotImplemented
	record()
	http.Error(w, "transport fixture unavailable", http.StatusNotImplemented)
}
