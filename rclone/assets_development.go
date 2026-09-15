//go:build !linux && (!darwin || !arm64)

package rclone

var embedded []byte

var binaryHash = ""

const archivePlatform = ""
