package events

import (
	"testing"

	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/protobuf/proto"
)

func TestRevisionFenceWireCompatibilityHelpers(t *testing.T) {
	witness := []byte("opaque-witness")
	request := &clientv1.CommitTransactionRequest{TransactionId: "tx-1"}
	revisionFenceAppendUnknownBytes(request, revisionFenceCommitField, witness)
	if got := revisionFenceUnknownBytes(request, revisionFenceCommitField); string(got) != string(witness) {
		t.Fatalf("commit witness = %q, want %q", got, witness)
	}
	metadata := &clientv1.ReadMetadata{}
	revisionFenceAppendUnknownBytes(metadata, revisionFenceWitnessField, witness)
	if got := revisionFenceUnknownBytes(metadata, revisionFenceWitnessField); string(got) != string(witness) {
		t.Fatalf("read witness before marshal = %q, want %q", got, witness)
	}
	encoded, err := proto.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &clientv1.ReadMetadata{}
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		t.Fatal(err)
	}
	if got := revisionFenceUnknownBytes(decoded, revisionFenceWitnessField); string(got) != string(witness) {
		t.Fatalf("read witness = %q, want %q", got, witness)
	}
}
