package restic

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
)

// syntheticDump looks like a SQL dump with an incompressible blob section, so chunks compress
// unevenly and back-references cross chunk boundaries.
func syntheticDump(size int) []byte {
	var dump bytes.Buffer
	random := rand.New(rand.NewSource(7))
	for row := 0; dump.Len() < size; row++ {
		if row%5000 == 4999 {
			blob := make([]byte, 64<<10)
			random.Read(blob)
			dump.Write(blob)
			continue
		}
		fmt.Fprintf(&dump, "INSERT INTO synthetic_records (id, name, total) VALUES (%d, 'synthetic-%d', %d.%02d);\n", row, row%977, random.Intn(100000), row%100)
	}

	return dump.Bytes()[:size]
}

func zipEntry(t *testing.T, workers int, contents []byte) ([]byte, uint64) {
	t.Helper()
	var output bytes.Buffer
	archive := newArchiveWriter(&output, workers)
	entry, err := archive.Create("synthetic.sql")
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(contents); offset += 300_000 {
		if _, err = entry.Write(contents[offset:min(offset+300_000, len(contents))]); err != nil {
			t.Fatal(err)
		}
	}
	if err = archive.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := zip.NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := reader.File[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	unpacked, err := io.ReadAll(opened)
	if err != nil {
		t.Fatalf("read back entry (CRC is checked at EOF): %v", err)
	}

	return unpacked, reader.File[0].CompressedSize64
}

func TestParallelDeflateProducesAValidEntryAsSmallAsSequentialCompression(t *testing.T) {
	dump := syntheticDump(5*deflateChunkBytes + 12345)

	unpacked, parallelSize := zipEntry(t, 3, dump)
	if !bytes.Equal(unpacked, dump) {
		t.Fatal("parallel entry did not unpack to the original")
	}

	_, sequentialSize := zipEntry(t, 1, dump)
	if float64(parallelSize) > float64(sequentialSize)*1.01 {
		t.Fatalf("parallel entry is %d bytes, sequential %d", parallelSize, sequentialSize)
	}
}

func TestParallelDeflateHandlesEntriesSmallerThanOneChunk(t *testing.T) {
	for _, size := range []int{0, 1, deflateChunkBytes - 1, deflateChunkBytes} {
		contents := syntheticDump(size)
		if unpacked, _ := zipEntry(t, 3, contents); !bytes.Equal(unpacked, contents) {
			t.Fatalf("%d bytes did not round-trip", size)
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("synthetic write failure")
}

func TestParallelDeflateReportsOutputFailures(t *testing.T) {
	deflater := newParallelDeflater(failingWriter{}, 2)
	_, err := deflater.Write(syntheticDump(3 * deflateChunkBytes))

	if err = errors.Join(err, deflater.Close()); err == nil {
		t.Fatal("accepted a failing output")
	}
}

func TestParallelDeflateStreamIsPlainDeflate(t *testing.T) {
	dump := syntheticDump(2*deflateChunkBytes + 99)
	var compressed bytes.Buffer
	deflater := newParallelDeflater(&compressed, 2)
	if _, err := deflater.Write(dump); err != nil {
		t.Fatal(err)
	}
	if err := deflater.Close(); err != nil {
		t.Fatal(err)
	}

	unpacked, err := io.ReadAll(flate.NewReader(&compressed))
	if err != nil || !bytes.Equal(unpacked, dump) {
		t.Fatalf("raw deflate reader: %v", err)
	}
}
