package uplog

import (
	"io"

	"github.com/golang/glog"
)

const (
	// When we create a new ByteBuffer, how big should the starting buffer be
	minBufferSize = 8192
)

// ByteBuffer enables expansion and sharing of the buffer
type ByteBuffer struct {
	data     []byte
	writePos int
	readPos  int
}

func (b *ByteBuffer) Clear() {
	b.writePos = 0
	b.readPos = 0
}

func (b *ByteBuffer) PeekAll() []byte {
	return b.data[b.readPos:b.writePos]
}

func (b *ByteBuffer) Skip(n int) {
	b.readPos += n
}

func (b *ByteBuffer) Available() int {
	return b.writePos - b.readPos
}

func (b *ByteBuffer) WriteFromReaderAt(f io.ReaderAt, offset int64) (int, error) {
	// Copy "remnant" bytes to the beginning of the buffer to maximize the read
	if b.readPos != 0 {
		n := b.writePos - b.readPos
		glog.Infof("leftover %d bytes", n)
		if n > 0 {
			copy(b.data[0:n], b.data[b.readPos:b.writePos])
			b.readPos = 0
			b.writePos = n
		} else {
			b.readPos = 0
			b.writePos = 0
		}
	}

	if b.writePos == cap(b.data) {
		// buffer is full - we can either enlarge the buffer or skip
		// TODO: Implement skipping for super-long lines
		oldsize := cap(b.data)
		newsize := oldsize * 2
		if newsize < minBufferSize {
			newsize = minBufferSize
		}
		glog.Warningf("growing buffer %d -> %d", oldsize, newsize)
		newbuffer := make([]byte, newsize, newsize)
		copy(newbuffer[0:oldsize], b.data[0:oldsize])
		b.data = newbuffer
	}

	dst := b.data[b.writePos:]
	// Note that ReadAt makes multiple calls to try to fill up the buffer for files
	// Two consequences: often returns io.EOF, and often a wasted syscall (?)
	n, err := f.ReadAt(dst, offset)
	if n != 0 {
		b.writePos += n
	}
	return n, err
}
