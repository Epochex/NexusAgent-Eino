//go:build !linux

package main

import "os/exec"

// Non-Linux is supported for local functional tests only. Production isolation
// is supplied by the separate Linux container, not a native subprocess claim.
func prepareChild(*exec.Cmd) {}
