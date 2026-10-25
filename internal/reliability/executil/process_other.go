//go:build !unix

package executil

import "os/exec"

func configureCommandCancellation(_ *exec.Cmd) {}
