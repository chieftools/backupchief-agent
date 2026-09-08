package restic

import _ "embed"

//go:embed restic_linux_amd64.bz2
var embedded []byte

var binaryHash = checksumFor("linux-amd64")
