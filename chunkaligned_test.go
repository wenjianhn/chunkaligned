package chunkaligned

import (
	"math/rand"
	"os"
	"reflect"
	"testing"
)

type fileGetter struct {
	size int64
	file *os.File
}

func (fg *fileGetter) Size() int64 {
	return fg.size
}

func (fg *fileGetter) ReadAt(p []byte, off int64) (n int, err error) {
	return fg.file.ReadAt(p, off)
}

func (fg *fileGetter) Close() error {
	return fg.file.Close()
}

func newFileGetter(path string) (fileGetter, error) {
	file, err := os.Open(path)
	if err != nil {
		return fileGetter{}, err
	}

	fs, err := file.Stat()
	if err != nil {
		return fileGetter{}, err
	}

	return fileGetter{fs.Size(), file}, nil
}

func TestNewChunkAlignedReaderAt(t *testing.T) {
	path := "/tmp/bin"

	fg, err := newFileGetter(path)
	defer fg.Close()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		chunkSize := 512 * 1024 * (2 << uint(i))
		for j := 0; j < 4; j++ {
			bufSize := 32 * 1024 * (2 << uint(j))
			for k := 0; k < 100; k++ {
				offset := rand.Int63n(fg.Size())
				cara, err := NewChunkAlignedReaderAt(&fg, chunkSize)
				if err != nil {
					t.Fatal(err)
				}

				bufActual := make([]byte, bufSize)
				cara.ReadAt(bufActual, offset)

				file, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				bufExpected := make([]byte, bufSize)
				file.ReadAt(bufExpected, offset)
				file.Close()

				if !reflect.DeepEqual(bufActual, bufExpected) {
					t.Fatal("bufActual not equal to bufExpected")
				}
				bufActual = nil
				bufExpected = nil
			}
		}
	}
}

// TODO(wenjianhn):
// func TestChunkSizeLimitExceeded(t *testing.T) {
