package provider

import (
	"errors"
	"gopkg.in/yaml.v3"
)

func (s *StageSpec) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return errors.New("provider stage must be an object")
	}
	seen := map[string]bool{}
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		switch key {
		case "name", "capability", "profileRef", "inputFrom":
		default:
			return errors.New("unknown provider stage field")
		}
		if seen[key] || node.Content[i+1].Tag != "!!str" {
			return errors.New("provider stage fields must be unique strings")
		}
		seen[key] = true
	}
	type plain StageSpec
	return node.Decode((*plain)(s))
}
