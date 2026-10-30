package provision

import "testing"

func TestCloneScenarioResourcesIsolatesCleanupStateAndPreservesCredentials(t *testing.T) {
	source := ScenarioResources{
		AdminPassword: "admin-secret",
		Assignments:   []ActorAssignment{{ActorID: "writer-0", Password: "actor-secret", AccessToken: "access-secret"}},
		Cleanup:       CleanupStatus{Errors: []string{"existing"}},
		Semantic: &SemanticResources{
			EvidenceValues: map[string]string{"key": "value"},
			Cleanup: []SemanticOperation{{Action: "delete", Details: map[string]any{
				"nested":     map[string]any{"value": "original"},
				"references": []map[string]string{{"type": "binding", "id": "binding-1"}},
			}, Attempts: []CleanupOperationAttempt{{Details: map[string]any{"status": "original"}}}}},
		},
	}
	clone := CloneScenarioResources(source)
	if clone.AdminPassword != "admin-secret" || clone.Assignments[0].Password != "actor-secret" || clone.Assignments[0].AccessToken != "access-secret" {
		t.Fatalf("clone lost cleanup credentials: %+v", clone)
	}
	clone.Assignments[0].Password = "changed"
	clone.Cleanup.Errors[0] = "changed"
	clone.Semantic.EvidenceValues["key"] = "changed"
	clone.Semantic.Cleanup[0].Details["nested"].(map[string]any)["value"] = "changed"
	clone.Semantic.Cleanup[0].Details["references"].([]map[string]string)[0]["id"] = "changed"
	clone.Semantic.Cleanup[0].Attempts[0].Details["status"] = "changed"
	if source.Assignments[0].Password != "actor-secret" || source.Cleanup.Errors[0] != "existing" || source.Semantic.EvidenceValues["key"] != "value" || source.Semantic.Cleanup[0].Details["nested"].(map[string]any)["value"] != "original" || source.Semantic.Cleanup[0].Details["references"].([]map[string]string)[0]["id"] != "binding-1" || source.Semantic.Cleanup[0].Attempts[0].Details["status"] != "original" {
		t.Fatalf("clone mutated source: %+v", source)
	}
}
