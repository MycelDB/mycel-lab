package provision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/MycelDB/mycel-lab/internal/reliability/spec"
	mycel "github.com/myceldb/mycel-go-sdk"
)

// CreateSemanticVariant provisions independent, owned inference resources for a
// same-space domain or alternate configured route. Partial state is registered
// before mutations so ordinary scenario cleanup can always reach it.
func CreateSemanticVariant(ctx context.Context, admin *mycel.AdminClient, resources *ScenarioResources, name, domainID string, route bool, dimensions int, vectorSpace string) (*SemanticResources, error) {
	if resources == nil || resources.Semantic == nil || resources.semanticPlan == nil || resources.Semantic.LiveProvider || len(resources.semanticPlan.Spec.ProviderStages) > 0 {
		return nil, errors.New("semantic variants require a provisioned credential-free direct fixture")
	}
	root := resources.Semantic
	if !spec.IsCanonicalSemanticUUID(root.SpaceID) || !spec.IsCanonicalSemanticUUID(domainID) || name == "" || len(root.DomainPeers)+len(root.Routes) >= 4 {
		return nil, errors.New("semantic variant has invalid scope or exceeds finite limit")
	}
	if route && domainID != root.DomainID || !route && domainID == root.DomainID {
		return nil, errors.New("semantic variant does not preserve the required domain relationship")
	}
	destination := root.DomainPeers
	if route {
		destination = root.Routes
	}
	if root.DomainPeers[name] != nil || root.Routes[name] != nil {
		return nil, errors.New("semantic variant must not overwrite an existing resource")
	}
	var actor ActorAssignment
	for _, assignment := range resources.Assignments {
		if assignment.ActorID == root.TargetActorID {
			actor = assignment
			break
		}
	}
	if actor.ActorID == "" || actor.SpaceID != root.SpaceID || actor.DomainID != root.DomainID || actor.PrincipalID != root.TargetPrincipalID {
		return nil, errors.New("semantic variant requires the same original principal and space")
	}
	actor.DomainID = domainID
	source := *resources.semanticPlan
	source.Spec.Rule.Key = root.SemanticRuleKey + "-" + name
	source.Spec.Rule.DisplayName = source.Spec.Rule.Key
	source.Spec.Rule.DeferProvisioning = true
	sum := sha256.Sum256([]byte(resources.RunID + "/" + root.SpaceID + "/" + name))
	suffix := hex.EncodeToString(sum[:8])
	resolved := spec.ResolvedScenario{Semantic: &source.Spec, ProviderProfile: source.ProviderProfile}
	peer, plan, err := planSemanticResources(resolved, ScenarioResources{Assignments: []ActorAssignment{actor}}, suffix)
	if err != nil {
		return nil, err
	}
	peer.SpaceID, peer.DomainID, peer.TargetPrincipalID = root.SpaceID, domainID, root.TargetPrincipalID
	if route {
		if dimensions < 1 || dimensions > 65536 || vectorSpace == "" {
			return nil, errors.New("configured route requires a positive vector shape")
		}
		plan.Capability.Dimensions, plan.Capability.VectorSpaceKey = dimensions, vectorSpace
	}
	if destination == nil {
		destination = map[string]*SemanticResources{}
	}
	destination[name] = peer
	if route {
		root.Routes = destination
	} else {
		root.DomainPeers = destination
	}
	view := *resources
	view.Semantic, view.semanticPlan = peer, plan
	view.Assignments = append([]ActorAssignment(nil), resources.Assignments...)
	for i := range view.Assignments {
		if view.Assignments[i].ActorID == actor.ActorID {
			view.Assignments[i] = actor
		}
	}
	if err := provisionSemanticResources(ctx, admin, &view, Options{}); err != nil {
		return peer, errors.New("semantic variant provisioning failed; mutations are not resubmitted")
	}
	return peer, nil
}

// SemanticScopeView changes only domain targeting for the original actor. It
// never switches principal, space, daemon, credentials, grants, or session token.
func SemanticScopeView(resources *ScenarioResources, name string) (*ScenarioResources, error) {
	if resources == nil || resources.Semantic == nil {
		return nil, errors.New("semantic resources are missing")
	}
	if name == "" {
		return resources, nil
	}
	root := resources.Semantic
	peer := root.DomainPeers[name]
	if peer == nil || peer.SpaceID != root.SpaceID || peer.DomainID == root.DomainID || peer.DomainID == "" || peer.TargetActorID != root.TargetActorID || peer.TargetPrincipalID != root.TargetPrincipalID || peer.TargetPrincipalID == "" {
		return nil, errors.New("semantic domain peer does not preserve original identity and space")
	}
	view := *resources
	view.Semantic = peer
	view.Assignments = append([]ActorAssignment(nil), resources.Assignments...)
	found := false
	for i := range view.Assignments {
		a := &view.Assignments[i]
		if a.ActorID == peer.TargetActorID {
			if a.SpaceID != root.SpaceID || a.DomainID != root.DomainID || a.PrincipalID != peer.TargetPrincipalID {
				return nil, errors.New("semantic domain actor scope is inconsistent")
			}
			a.DomainID = peer.DomainID
			found = true
		}
	}
	if !found {
		return nil, errors.New("semantic domain actor is unavailable")
	}
	// The same immutable rule blueprint applies to the peer. Only its semantic
	// key/display name differ; infer them from the provisioned peer resource.
	if resources.semanticPlan != nil {
		plan := *resources.semanticPlan
		plan.Spec.Rule.Key, plan.Spec.Rule.DisplayName = peer.SemanticRuleKey, peer.SemanticRuleKey
		view.semanticPlan = &plan
	}
	return &view, nil
}
