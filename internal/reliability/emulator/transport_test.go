package emulator

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

type evidenceCloseConn struct {
	net.Conn
	beforeClose func()
	closes      int
}

func (c *evidenceCloseConn) Close() error { c.beforeClose(); c.closes++; return nil }

type evidenceHijacker struct {
	*httptest.ResponseRecorder
	connection net.Conn
}

func (w evidenceHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.connection, nil, nil
}

func TestConnectionDropRecordsExactlyOnceBeforeClose(t *testing.T) {
	recorded := 0
	observation := Observation{Outcome: "connection-drop"}
	connection := &evidenceCloseConn{beforeClose: func() {
		if recorded != 1 || observation.Status != http.StatusServiceUnavailable {
			t.Fatal("EOF became visible before its finite transport evidence")
		}
	}}
	response := httptest.NewRecorder()
	dropProviderConnection(evidenceHijacker{ResponseRecorder: response, connection: connection}, &observation, func() { recorded++ })
	if recorded != 1 || connection.closes != 1 || response.Body.Len() != 0 {
		t.Fatal("drop duplicated evidence, failed to close, or wrote an HTTP response")
	}
}
func TestUnsupportedTransportIsRecordedAndFailsClosed(t *testing.T) {
	recorded := 0
	observation := Observation{Outcome: "connection-drop"}
	response := httptest.NewRecorder()
	dropProviderConnection(response, &observation, func() { recorded++ })
	if recorded != 1 || response.Code != http.StatusNotImplemented || observation.Outcome != "unsupported-transport" {
		t.Fatal("unsupported transport masqueraded as a real connection drop")
	}
}
