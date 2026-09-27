package blobstore

import (
	"errors"
	"io"
	"net/http"
)

// receiveBufSize is the copy chunk. 64 KiB is large enough that the per-read
// overhead is noise against a multi-megabyte upload and small enough that the
// limit below is enforced within one chunk of the ceiling.
const receiveBufSize = 64 * 1024

// ErrTooLarge is the wire-limit failure: the stream passed the group's
// max_blob_bytes.
//
// IT IS A SEPARATE ERROR because it is the one failure the response has to
// distinguish (413, not 400): the client declared a size that was inside the
// limit and then sent more, and the difference between "you lied about the size"
// and "your bytes did not hash as promised" is the difference between a
// misconfigured client and a corrupted transfer.
var ErrTooLarge = errors.New("blobstore: upload exceeds the volume group's max_blob_bytes")

// receive copies an upload's body into dst, hashing as it goes, and returns the
// byte count, the hex digest, and the head of the stream for the magic-byte check.
//
// THE LIMIT IS ENFORCED ON THE WIRE, and this is where (R9 step 2). It is not a
// duplicate of CreateUpload's declared-size check: that one refuses a caller who
// ASKS for more than the group allows, and this one refuses a caller who asked
// for less and then sent more. Without it, a client that declares 1 KiB can fill
// the PVC. The abort happens at the limit — the bytes past it are never written.
//
// dst is a single writer so that the caller decides what the bytes go to: the
// public path tees to the local temp file AND the peer, the peer path writes only
// its own temp file. Neither variant needs its own copy loop.
func receive(dst io.Writer, body io.Reader, maxBytes int64) (size int64, hexDigest string, head []byte, err error) {
	h := newHasher()
	head = make([]byte, 0, sniffBytes)
	buf := make([]byte, receiveBufSize)
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			if size+int64(n) > maxBytes {
				return size, "", head, ErrTooLarge
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return size, "", head, werr
			}
			// The hasher cannot fail (hash.Hash's Write never returns an error),
			// so its result is not checked — the io.Writer signature is the only
			// reason it returns one at all.
			_, _ = h.Write(buf[:n])
			if len(head) < sniffBytes {
				take := sniffBytes - len(head)
				if take > n {
					take = n
				}
				head = append(head, buf[:take]...)
			}
			size += int64(n)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return size, "", head, rerr
		}
	}
	return size, h.hex(), head, nil
}

// countingWriter counts what actually reached the client, which is what
// BlobLinkStats.bytes_served means (R10) — for a Range request that is the range,
// not the file.
type countingWriter struct {
	inner   io.Writer
	written int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.inner.Write(p)
	c.written += int64(n)
	return n, err
}

// countingResponseWriter is a http.ResponseWriter whose body writes are counted.
//
// It exists because http.ServeContent — which is what gives `/b/` its Range
// support (R8) — writes the body itself, so the only place to count what went out
// is the writer it is handed. Header and WriteHeader still go to the real
// ResponseWriter, so the response is byte-for-byte what ServeContent intended.
type countingResponseWriter struct {
	http.ResponseWriter
	counter *countingWriter
}

func (c countingResponseWriter) Write(p []byte) (int, error) { return c.counter.Write(p) }

// responseWriterWith wraps w so its body writes land in counter.
func responseWriterWith(w http.ResponseWriter, counter *countingWriter) http.ResponseWriter {
	return countingResponseWriter{ResponseWriter: w, counter: counter}
}
