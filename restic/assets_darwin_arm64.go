package restic

import _ "embed"

//go:embed restic_darwin_arm64.bz2
var embedded []byte

var binaryHash = checksumFor("darwin-arm64")
