//go:build linux

package main

import (
	"os/exec"
	"syscall"
)

func prepareChild(cmd *exec.Cmd) {
	// A parent killed by a container OOM must not leave a parsing child behind.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}
