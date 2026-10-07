package restic

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"sync"
)

const (
	deflateChunkBytes  = 1 << 20
	deflateWindowBytes = 32 << 10
)

// finalDeflateBlock is an empty fixed-Huffman block with the final bit set, which ends a stream
// made of sync-flushed chunks.
var finalDeflateBlock = []byte{0x03, 0x00}

// parallelDeflater compresses one ZIP entry on several cores, like pigz: the input is cut into
// chunks that are deflated side by side and written in order as one raw deflate stream. Each
// chunk ends on a byte boundary with a sync flush and is primed with the last 32 KiB of the
// chunk before it, so the result stays practically as small as a sequential stream. Entries
// smaller than one chunk are compressed inline without starting any workers.
type parallelDeflater struct {
	output  io.Writer
	workers int

	chunk   []byte
	window  []byte
	started bool
	closed  bool

	jobs    chan deflateJob
	ordered chan chan []byte
	written chan struct{}
	group   sync.WaitGroup

	mu  sync.Mutex
	err error
}

type deflateJob struct {
	input      []byte
	dictionary []byte
	result     chan []byte
}

func newParallelDeflater(output io.Writer, workers int) *parallelDeflater {
	return &parallelDeflater{
		output:  output,
		workers: workers,
		chunk:   make([]byte, 0, deflateChunkBytes),
	}
}

func (deflater *parallelDeflater) Write(data []byte) (int, error) {
	if err := deflater.failure(); err != nil {
		return 0, err
	}

	written := 0
	for written < len(data) {
		count := copy(deflater.chunk[len(deflater.chunk):cap(deflater.chunk)], data[written:])
		deflater.chunk = deflater.chunk[:len(deflater.chunk)+count]
		written += count

		if len(deflater.chunk) == deflateChunkBytes {
			deflater.dispatch()
			if err := deflater.failure(); err != nil {
				return written, err
			}
		}
	}

	return written, nil
}

func (deflater *parallelDeflater) Close() error {
	if deflater.closed {
		return deflater.failure()
	}
	deflater.closed = true

	if !deflater.started {
		writer, err := flate.NewWriter(deflater.output, flate.BestSpeed)
		if err != nil {
			return err
		}
		if _, err = writer.Write(deflater.chunk); err != nil {
			return err
		}

		return writer.Close()
	}

	if len(deflater.chunk) > 0 {
		deflater.dispatch()
	}
	close(deflater.jobs)
	close(deflater.ordered)
	deflater.group.Wait()
	<-deflater.written

	if err := deflater.failure(); err != nil {
		return err
	}
	if _, err := deflater.output.Write(finalDeflateBlock); err != nil {
		return err
	}

	return nil
}

// dispatch hands the full chunk to a worker; the bounded queue keeps memory to a few chunks
// per worker when compression falls behind.
func (deflater *parallelDeflater) dispatch() {
	if !deflater.started {
		deflater.start()
	}

	job := deflateJob{input: deflater.chunk, dictionary: deflater.window, result: make(chan []byte, 1)}
	deflater.ordered <- job.result
	deflater.jobs <- job

	deflater.window = append([]byte(nil), deflater.chunk[max(0, len(deflater.chunk)-deflateWindowBytes):]...)
	deflater.chunk = make([]byte, 0, deflateChunkBytes)
}

func (deflater *parallelDeflater) start() {
	deflater.started = true
	deflater.jobs = make(chan deflateJob, deflater.workers)
	deflater.ordered = make(chan chan []byte, deflater.workers*2)
	deflater.written = make(chan struct{})

	for range deflater.workers {
		deflater.group.Add(1)
		go func() {
			defer deflater.group.Done()
			for job := range deflater.jobs {
				job.result <- deflater.compress(job)
			}
		}()
	}

	go func() {
		defer close(deflater.written)
		for result := range deflater.ordered {
			compressed := <-result
			if compressed == nil || deflater.failure() != nil {
				continue
			}
			if _, err := deflater.output.Write(compressed); err != nil {
				deflater.fail(err)
			}
		}
	}()
}

func (deflater *parallelDeflater) compress(job deflateJob) []byte {
	var buffer bytes.Buffer
	buffer.Grow(len(job.input) / 2)

	writer, err := flate.NewWriterDict(&buffer, flate.BestSpeed, job.dictionary)
	if err == nil {
		_, err = writer.Write(job.input)
	}
	if err == nil {
		err = writer.Flush()
	}
	if err != nil {
		deflater.fail(errors.New("cannot compress archive entry"))
		return nil
	}

	return buffer.Bytes()
}

func (deflater *parallelDeflater) fail(err error) {
	deflater.mu.Lock()
	if deflater.err == nil {
		deflater.err = err
	}
	deflater.mu.Unlock()
}

func (deflater *parallelDeflater) failure() error {
	deflater.mu.Lock()
	defer deflater.mu.Unlock()

	return deflater.err
}
