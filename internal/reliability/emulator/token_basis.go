package emulator

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
)

// TokenBasisV1 independently implements the published deterministic fixture
// algorithm so protocol-emulator results remain comparable to Tier 0.
func TokenBasisV1(input string, dimensions int) []float64 {
	normalized := normalizeInput(input)
	vector := make([]float64, dimensions)
	recognized := false
	for _, token := range strings.FieldsFunc(normalized, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-')
	}) {
		if strings.HasPrefix(token, "axis-") {
			rawAxis := strings.TrimPrefix(token, "axis-")
			if axis, err := strconv.Atoi(rawAxis); err == nil && rawAxis != "" && (len(rawAxis) == 1 || rawAxis[0] != '0') && axis >= 0 && axis < dimensions {
				vector[axis]++
				recognized = true
			}
		}
	}
	if !recognized {
		for offset, block := 0, uint32(0); offset < len(vector); block++ {
			hasher := sha256.New()
			_, _ = hasher.Write([]byte(normalized))
			_, _ = hasher.Write([]byte{0})
			var suffix [4]byte
			binary.BigEndian.PutUint32(suffix[:], block)
			_, _ = hasher.Write(suffix[:])
			digest := hasher.Sum(nil)
			for index := 0; index+4 <= len(digest) && offset < len(vector); index, offset = index+4, offset+1 {
				value := int32(binary.BigEndian.Uint32(digest[index : index+4]))
				vector[offset] = float64(value) / 2147483648.0
			}
		}
	}
	var squared float64
	for _, value := range vector {
		squared += value * value
	}
	if squared == 0 {
		vector[0] = 1
		return vector
	}
	norm := math.Sqrt(squared)
	for index := range vector {
		vector[index] /= norm
	}
	return vector
}

func normalizeInput(input string) string {
	var out strings.Builder
	out.Grow(len(input))
	space := true
	for index := 0; index < len(input); index++ {
		value := input[index]
		switch value {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			if !space {
				out.WriteByte(' ')
				space = true
			}
		default:
			if value >= 'A' && value <= 'Z' {
				value += 'a' - 'A'
			}
			out.WriteByte(value)
			space = false
		}
	}
	return strings.TrimSpace(out.String())
}
