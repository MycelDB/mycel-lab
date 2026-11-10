package events

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	revisionFenceWitnessField  protowire.Number = 9 // ReadMetadata.revision_witness
	revisionFenceCommitField   protowire.Number = 5 // CommitTransactionRequest.revision_witnesses
	revisionFenceTargetNode                     = "00000000-0000-4000-8000-000000000201"
	revisionFenceUnrelatedNode                  = "00000000-0000-4000-8000-000000000202"
)

// executeRevisionFenceAssertion exercises the public gRPC path without relying
// on the Lab process having regenerated SDK bindings. The compatibility helper
// writes/reads the additive protobuf fields as unknown wire fields; once the
// SDK release containing those fields is consumed, this remains wire-equivalent.
func executeRevisionFenceAssertion(ctx context.Context, resources *provision.ScenarioResources, target map[string]any) (map[string]any, error) {
	actorID := strings.TrimSpace(fmt.Sprint(target["actorId"]))
	if actorID == "" || actorID == "<nil>" {
		return nil, errors.New("revision-fence-assert requires target.actorId")
	}
	client, assignment, err := dialGraphActorGroup(ctx, resources, "", actorID)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	if assignment.SpaceID == "" || assignment.DomainID == "" {
		return nil, errors.New("revision-fence-assert actor assignment has no graph scope")
	}

	if err := revisionFenceSeed(ctx, client, assignment); err != nil {
		return nil, fmt.Errorf("revision fence seed: %w", err)
	}
	witness, err := revisionFenceReadWitness(ctx, client, assignment, revisionFenceTargetNode)
	if err != nil {
		return nil, fmt.Errorf("revision fence read: %w", err)
	}
	if len(witness) == 0 {
		return nil, errors.New("revision fence read returned no witness")
	}

	// Control: changing an unrelated node must not invalidate the target witness.
	if err := revisionFenceUpdateAndCommit(ctx, client, assignment, revisionFenceUnrelatedNode, "unrelated-update", nil); err != nil {
		return nil, fmt.Errorf("revision fence unrelated control: %w", err)
	}
	if err := revisionFenceUpdateAndCommit(ctx, client, assignment, revisionFenceTargetNode, "derived-from-stable-read", [][]byte{witness}); err != nil {
		return nil, fmt.Errorf("revision fence control commit: %w", err)
	}

	freshWitness, err := revisionFenceReadWitness(ctx, client, assignment, revisionFenceTargetNode)
	if err != nil {
		return nil, fmt.Errorf("revision fence second read: %w", err)
	}
	if err := revisionFenceUpdateAndCommit(ctx, client, assignment, revisionFenceTargetNode, "concurrent-update", nil); err != nil {
		return nil, fmt.Errorf("revision fence concurrent update: %w", err)
	}

	conflictErr := revisionFenceUpdateAndCommit(ctx, client, assignment, revisionFenceTargetNode, "stale-derived-output", [][]byte{freshWitness})
	if conflictErr == nil {
		return nil, errors.New("revision fence stale commit unexpectedly succeeded")
	}
	if status.Code(conflictErr) != codes.Aborted || !hasRevisionFenceConflictDetail(conflictErr) {
		return nil, fmt.Errorf("revision fence stale commit returned unexpected status: %s", revisionFenceErrorClass(conflictErr))
	}
	finalNode, err := revisionFenceReadNode(ctx, client, assignment, revisionFenceTargetNode)
	if err != nil {
		return nil, fmt.Errorf("revision fence final read: %w", err)
	}
	finalText := ""
	if finalNode != nil && finalNode.GetPayload() != nil {
		if value, ok := finalNode.GetPayload().GetFields()["text"]; ok {
			finalText = value.GetStringValue()
		}
	}
	if finalText != "concurrent-update" {
		return nil, errors.New("revision fence stale output became visible")
	}
	return map[string]any{
		"controlCommitSucceeded":       true,
		"conflictCode":                 status.Code(conflictErr).String(),
		"conflictReason":               "GRAPH_REVISION_FENCE_CONFLICT",
		"readWitnessPresent":           true,
		"readWitnessBytesBounded":      len(freshWitness) <= 131072,
		"staleOutputNotVisible":        true,
		"witnessBytesRedacted":         true,
		"revisionFenceAssertionPassed": true,
	}, nil
}

func revisionFenceSeed(ctx context.Context, client *mycel.Client, assignment provision.ActorAssignment) error {
	sessionID, err := client.OpenSession(ctx, assignment.SpaceID, assignment.DomainID)
	if err != nil {
		return err
	}
	defer client.CloseSession(context.Background(), sessionID)
	txID, err := client.BeginReadWriteTransaction(ctx, sessionID)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = client.CloseTransaction(context.Background(), txID)
		}
	}()
	for _, nodeID := range []string{revisionFenceTargetNode, revisionFenceUnrelatedNode} {
		if _, err := client.CreateNode(ctx, txID, &clientv1.NodeCreate{NodeId: &nodeID, Labels: []string{"Note"}}); err != nil {
			return err
		}
	}
	if err := client.CommitTransaction(ctx, txID); err != nil {
		return err
	}
	closed = true
	return nil
}

func revisionFenceReadWitness(ctx context.Context, client *mycel.Client, assignment provision.ActorAssignment, nodeID string) ([]byte, error) {
	sessionID, err := client.OpenSession(ctx, assignment.SpaceID, assignment.DomainID)
	if err != nil {
		return nil, err
	}
	defer client.CloseSession(context.Background(), sessionID)
	txID, err := client.BeginReadOnlyTransaction(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	defer client.CloseTransaction(context.Background(), txID)
	callCtx, cancel := client.AuthCallContext(ctx)
	defer cancel()
	response, err := client.Graph.GetNode(callCtx, &clientv1.GetNodeRequest{TransactionId: txID, NodeId: nodeID})
	if err != nil {
		return nil, err
	}
	metadata := response.GetReadMetadata()
	witness := revisionFenceUnknownBytes(metadata, revisionFenceWitnessField)
	if len(witness) == 0 {
		metadataPresent := metadata != nil
		unknownBytes := 0
		if metadata != nil {
			unknownBytes = len(metadata.ProtoReflect().GetUnknown())
		}
		return nil, fmt.Errorf("read returned no witness (metadata_present=%t unknown_bytes=%d)", metadataPresent, unknownBytes)
	}
	return witness, nil
}

func revisionFenceUpdateAndCommit(ctx context.Context, client *mycel.Client, assignment provision.ActorAssignment, nodeID, text string, witnesses [][]byte) error {
	sessionID, err := client.OpenSession(ctx, assignment.SpaceID, assignment.DomainID)
	if err != nil {
		return err
	}
	defer client.CloseSession(context.Background(), sessionID)
	txID, err := client.BeginReadWriteTransaction(ctx, sessionID)
	if err != nil {
		return err
	}
	defer client.CloseTransaction(context.Background(), txID)
	if _, err := client.UpdateNodeContent(ctx, txID, nodeID, text); err != nil {
		return err
	}
	callCtx, cancel := client.AuthCallContext(ctx)
	defer cancel()
	request := &clientv1.CommitTransactionRequest{TransactionId: txID}
	for _, witness := range witnesses {
		revisionFenceAppendUnknownBytes(request, revisionFenceCommitField, witness)
	}
	_, err = client.Transaction.CommitTransaction(callCtx, request)
	return err
}

func revisionFenceReadNode(ctx context.Context, client *mycel.Client, assignment provision.ActorAssignment, nodeID string) (*clientv1.Node, error) {
	sessionID, err := client.OpenSession(ctx, assignment.SpaceID, assignment.DomainID)
	if err != nil {
		return nil, err
	}
	defer client.CloseSession(context.Background(), sessionID)
	txID, err := client.BeginReadOnlyTransaction(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	defer client.CloseTransaction(context.Background(), txID)
	return client.GetNode(ctx, txID, nodeID)
}

func revisionFenceUnknownBytes(message interface{ ProtoReflect() protoreflect.Message }, field protowire.Number) []byte {
	if message == nil {
		return nil
	}
	reflected := message.ProtoReflect()
	if descriptor := reflected.Descriptor().Fields().ByNumber(field); descriptor != nil && descriptor.Kind() == protoreflect.BytesKind {
		if descriptor.IsList() {
			list := reflected.Get(descriptor).List()
			if list.Len() > 0 {
				return append([]byte(nil), list.Get(list.Len()-1).Bytes()...)
			}
		} else if reflected.Has(descriptor) {
			return append([]byte(nil), reflected.Get(descriptor).Bytes()...)
		}
	}
	unknown := reflected.GetUnknown()
	for len(unknown) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(unknown)
		if consumed < 0 {
			return nil
		}
		unknown = unknown[consumed:]
		if number == field && wireType == protowire.BytesType {
			value, size := protowire.ConsumeBytes(unknown)
			if size < 0 {
				return nil
			}
			return append([]byte(nil), value...)
		}
		size := protowire.ConsumeFieldValue(number, wireType, unknown)
		if size < 0 {
			return nil
		}
		unknown = unknown[size:]
	}
	return nil
}

func revisionFenceAppendUnknownBytes(message interface{ ProtoReflect() protoreflect.Message }, field protowire.Number, value []byte) {
	if message == nil || len(value) == 0 {
		return
	}
	reflected := message.ProtoReflect()
	if descriptor := reflected.Descriptor().Fields().ByNumber(field); descriptor != nil && descriptor.Kind() == protoreflect.BytesKind {
		if descriptor.IsList() {
			reflected.Mutable(descriptor).List().Append(protoreflect.ValueOfBytes(append([]byte(nil), value...)))
		} else {
			reflected.Set(descriptor, protoreflect.ValueOfBytes(append([]byte(nil), value...)))
		}
		return
	}
	unknown := reflected.GetUnknown()
	unknown = protowire.AppendTag(unknown, field, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, value)
	message.ProtoReflect().SetUnknown(unknown)
}

func hasRevisionFenceConflictDetail(err error) bool {
	for _, detail := range status.Convert(err).Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && info.GetReason() == "GRAPH_REVISION_FENCE_CONFLICT" {
			return true
		}
	}
	return false
}

func revisionFenceErrorClass(err error) string {
	if err == nil {
		return ""
	}
	return status.Code(err).String()
}
