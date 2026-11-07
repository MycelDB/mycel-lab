package events

import (
	"context"
	"errors"

	"github.com/MycelDB/mycel-lab/internal/reliability/provision"
)

type SemanticDomainIsolationEvidence struct {
	SchemaVersion           int                    `json:"schemaVersion"`
	SpaceID                 string                 `json:"spaceId"`
	PrincipalID             string                 `json:"principalId"`
	PrimaryDomainID         string                 `json:"primaryDomainId"`
	PeerDomainID            string                 `json:"peerDomainId"`
	SharedNodeID            string                 `json:"sharedNodeId"`
	Primary                 SemanticSearchEvidence `json:"primary"`
	Peer                    SemanticSearchEvidence `json:"peer"`
	CrossDomainResultCounts []int                  `json:"crossDomainResultCounts"`
}

func executeSemanticDomainIsolation(ctx context.Context, resources *provision.ScenarioResources, target map[string]any) (SemanticDomainIsolationEvidence, error) {
	result := SemanticDomainIsolationEvidence{SchemaVersion: 1}
	if resources == nil || resources.Semantic == nil || resources.Semantic.LiveProvider {
		return result, errors.New("domain isolation requires a credential-free fixture")
	}
	peer, err := provision.SemanticScopeView(resources, targetString(target, "peer"))
	if err != nil {
		return result, err
	}
	a, b := resources.Semantic, peer.Semantic
	if a == b || a.SemanticRuleID == "" || b.SemanticRuleID == "" || a.SemanticRuleID == b.SemanticRuleID || a.TargetPrincipalID == "" || a.TargetPrincipalID != b.TargetPrincipalID || a.SpaceID != b.SpaceID || a.DomainID == b.DomainID {
		return result, errors.New("isolation must vary only domain, not principal or space")
	}
	timeout := durationTarget(target, "timeout", a.MaintenanceTimeout)
	if timeout <= 0 {
		return result, errors.New("domain isolation timeout must be bounded")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result.SpaceID, result.PrincipalID, result.PrimaryDomainID, result.PeerDomainID, result.SharedNodeID = a.SpaceID, a.TargetPrincipalID, a.DomainID, b.DomainID, targetString(target, "nodeId")
	queries := []string{targetString(target, "primaryQuery"), targetString(target, "peerQuery")}
	if queries[0] == queries[1] {
		return result, errors.New("domain fixture queries must be distinguishable")
	}
	views := []*provision.ScenarioResources{resources, peer}
	revisions := []string{targetString(target, "expectPrimaryRevisionFrom"), targetString(target, "expectPeerRevisionFrom")}
	captures := []string{targetString(target, "capturePrimaryRecordAs"), targetString(target, "capturePeerRecordAs")}
	expected := []string{targetString(target, "expectPrimaryRecordFrom"), targetString(target, "expectPeerRecordFrom")}
	proofs := make([]SemanticSearchEvidence, 2)
	for i, view := range views {
		query := map[string]any{"query": queries[i], "broad": true, "expectedResultCount": 1, "expectedNodeIds": []any{result.SharedNodeID}, "minimumScore": 0.99, "requireProvenance": true, "requireConfiguredRoute": true, "expectedPayloadText": queries[i], "expectSourceRevisionFrom": revisions[i], "timeout": timeout.String()}
		if captures[i] != "" {
			query["captureRecordAs"] = captures[i]
		}
		if expected[i] != "" {
			query["expectRecordFrom"] = expected[i]
		}
		proof, err := executeSemanticSearchAssertion(ctx, view, query)
		if err != nil {
			return result, errors.New("domain-local positive search/provenance/hydration failed")
		}
		proofs[i] = proof
		cross, err := executeSemanticSearchAssertion(ctx, view, map[string]any{"query": queries[1-i], "broad": true, "minimumScore": 0.99, "expectedResultCount": 0, "forbiddenNodeIds": []any{result.SharedNodeID}, "timeout": timeout.String()})
		if err != nil {
			return result, errors.New("broad semantic search leaked across same-space domains")
		}
		result.CrossDomainResultCounts = append(result.CrossDomainResultCounts, len(cross.Results))
	}
	if len(proofs[0].Results) != 1 || len(proofs[1].Results) != 1 || proofs[0].Results[0].RecordID == proofs[1].Results[0].RecordID {
		return result, errors.New("independent same-node-ID domain indexes were not proved")
	}
	result.Primary, result.Peer = proofs[0], proofs[1]
	return result, nil
}
