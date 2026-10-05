package actors

import (
	"context"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func assignmentCanUseRealClient(assignment provision.ActorAssignment) bool {
	return assignment.DaemonAddr != "" && assignment.SpaceID != "" && assignment.DomainID != "" && (assignment.AccessToken != "" || (assignment.Username != "" && assignment.Password != ""))
}

func assignmentCanReauthenticate(assignment provision.ActorAssignment) bool {
	return assignment.DaemonAddr != "" && assignment.Username != "" && assignment.Password != ""
}

func clientConfigForAssignment(assignment provision.ActorAssignment, forceLogin bool) mycel.Config {
	cfg := mycel.Config{Addr: assignment.DaemonAddr, Username: assignment.Username, Password: assignment.Password, ClientName: "mycel-lab"}
	if !forceLogin && assignment.AccessToken != "" {
		cfg.Username = ""
		cfg.Password = ""
		cfg.AccessToken = assignment.AccessToken
		cfg.RefreshToken = assignment.RefreshToken
		cfg.AccessTokenExpireTime = assignment.TokenExpires
	}
	return cfg
}

func isUnauthenticatedError(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.Unauthenticated {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unauthenticated") || strings.Contains(msg, "authorization token is invalid") || strings.Contains(msg, "authorization token is expired")
}

func shouldReauthenticate(err error, assignment provision.ActorAssignment) bool {
	return isUnauthenticatedError(err) && assignmentCanReauthenticate(assignment)
}

func captureClientTokens(assignment *provision.ActorAssignment, client *mycel.Client) {
	if assignment == nil || client == nil {
		return
	}
	if token := client.AccessToken(); token != "" {
		assignment.AccessToken = token
		assignment.TokenExpires = client.AccessTokenExpireTime()
	}
	if refresh := client.RefreshToken(); refresh != "" {
		assignment.RefreshToken = refresh
	}
}

func recordReauthenticationEvent(ctx context.Context, recorder EventRecorder, eventType string, actorID string, groupName string, payload map[string]any) {
	if recorder == nil {
		return
	}
	if payload == nil {
		payload = map[string]any{}
	}
	_ = recorder.RecordActorEvent(ctx, ActorEvent{Type: eventType, ActorID: actorID, GroupName: groupName, Payload: payload})
}
