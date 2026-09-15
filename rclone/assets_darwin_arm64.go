package rclone

import _ "embed"

//go:embed rclone_osx_arm64.zip
var embedded []byte

var binaryHash = checksumFor("darwin-arm64")

const archivePlatform = "osx-arm64"
