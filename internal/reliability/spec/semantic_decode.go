package spec

import (
	"bytes"
	"gopkg.in/yaml.v3"
)

// Semantic fields are execution contracts. In particular, a misspelled staged
// composition must not be ignored and silently become embedding-only.
func (s *SemanticScenarioSpec) UnmarshalYAML(node *yaml.Node) error {
	raw, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	type plain SemanticScenarioSpec
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	return decoder.Decode((*plain)(s))
}
