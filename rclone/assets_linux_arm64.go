package rclone

import _ "embed"

//go:embed rclone_linux_arm64.zip
var embedded []byte

var binaryHash = checksumFor("linux-arm64")

const archivePlatform = "linux-arm64"
