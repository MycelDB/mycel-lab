package emulator

import "net/http"

func dropProviderConnection(w http.ResponseWriter, observation *Observation) {
	if hijacker, ok := w.(http.Hijacker); ok {
		connection, _, err := hijacker.Hijack()
		if err == nil {
			observation.Status = http.StatusServiceUnavailable // synthetic envelope; no HTTP response was sent
			_ = connection.Close()
			return
		}
	}
	observation.Outcome, observation.Status = "unsupported-transport", http.StatusNotImplemented
	http.Error(w, "transport fixture unavailable", http.StatusNotImplemented)
}
