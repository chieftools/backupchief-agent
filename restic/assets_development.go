//go:build !linux && (!darwin || !arm64)

package restic

var embedded []byte

var binaryHash = ""
