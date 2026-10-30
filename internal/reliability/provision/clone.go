package provision

// CloneScenarioResources isolates cleanup bookkeeping while preserving the
// credentials and private plans required by cleanup operations.
func CloneScenarioResources(source ScenarioResources) ScenarioResources {
	clone := source
	clone.DaemonAddrs = append([]string(nil), source.DaemonAddrs...)
	clone.Principals = append([]Principal(nil), source.Principals...)
	clone.Spaces = append([]Space(nil), source.Spaces...)
	clone.Domains = append([]Domain(nil), source.Domains...)
	clone.Assignments = append([]ActorAssignment(nil), source.Assignments...)
	clone.Cleanup.Errors = append([]string(nil), source.Cleanup.Errors...)
	clone.Cleanup.ErrorCategories = append([]string(nil), source.Cleanup.ErrorCategories...)
	if source.Semantic != nil {
		semantic := *source.Semantic
		semantic.Operations = cloneSemanticOperations(source.Semantic.Operations)
		semantic.Cleanup = cloneSemanticOperations(source.Semantic.Cleanup)
		semantic.EvidenceValues = cloneStringMap(source.Semantic.EvidenceValues)
		semantic.ProviderBudgetCapabilities = append([]string(nil), source.Semantic.ProviderBudgetCapabilities...)
		semantic.cleanupProof = cloneCleanupProof(source.Semantic.cleanupProof)
		clone.Semantic = &semantic
	}
	if source.ProcedureInference != nil {
		procedure := *source.ProcedureInference
		procedure.TargetLabels = append([]string(nil), source.ProcedureInference.TargetLabels...)
		procedure.Operations = cloneSemanticOperations(source.ProcedureInference.Operations)
		procedure.Cleanup = cloneSemanticOperations(source.ProcedureInference.Cleanup)
		procedure.cleanupProof = cloneCleanupProof(source.ProcedureInference.cleanupProof)
		clone.ProcedureInference = &procedure
	}
	return clone
}

func cloneSemanticOperations(source []SemanticOperation) []SemanticOperation {
	clone := append([]SemanticOperation(nil), source...)
	for index := range clone {
		clone[index].Details = cloneAnyMap(source[index].Details)
		clone[index].Attempts = append([]CleanupOperationAttempt(nil), source[index].Attempts...)
		for attempt := range clone[index].Attempts {
			clone[index].Attempts[attempt].Details = cloneAnyMap(source[index].Attempts[attempt].Details)
		}
	}
	return clone
}

func cloneCleanupProof(source *procedureInferenceCleanupProof) *procedureInferenceCleanupProof {
	if source == nil {
		return nil
	}
	clone := &procedureInferenceCleanupProof{nodes: make(map[string]procedureInferenceCleanupProofNode, len(source.nodes))}
	for key, node := range source.nodes {
		node.Details = cloneAnyMap(node.Details)
		node.Dependencies = append([]procedureInferenceCleanupProofDependency(nil), node.Dependencies...)
		clone.nodes[key] = node
	}
	return clone
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneAnyMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = cloneAny(value)
	}
	return clone
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case []any:
		clone := make([]any, len(typed))
		for index := range typed {
			clone[index] = cloneAny(typed[index])
		}
		return clone
	case map[string]string:
		return cloneStringMap(typed)
	case []map[string]string:
		clone := make([]map[string]string, len(typed))
		for index := range typed {
			clone[index] = cloneStringMap(typed[index])
		}
		return clone
	case []string:
		return append([]string(nil), typed...)
	default:
		return value
	}
}
