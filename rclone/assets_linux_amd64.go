package rclone

import _ "embed"

//go:embed rclone_linux_amd64.zip
var embedded []byte

var binaryHash = checksumFor("linux-amd64")

const archivePlatform = "linux-amd64"
