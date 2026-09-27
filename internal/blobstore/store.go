package blobstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// tmpDirName is the subdirectory partial uploads are written into, under the
// PVC root. It sits INSIDE the data dir rather than in /tmp so that the rename
// into place is a rename within one filesystem — a cross-device rename is a copy
// and is not atomic, which is the whole property promotion depends on.
const tmpDirName = "tmp"

// Store is one pod's PVC: content-addressed files under
// <data_dir>/<group>/<ns>/<sha256-hex>, plus the tmp directory.
//
// It knows nothing about records, tokens, groups or peers. A file either has its
// hash or does not (R2), and that is the entire contract this type implements.
type Store struct {
	root string
}

// NewStore prepares the data directory and clears anything left in tmp.
//
// The tmp sweep at start-up is not housekeeping, it is the recovery half of the
// upload protocol: a crash mid-upload leaves a temp file and nothing else
// deletes it (R2, R9 step 4). A partial file is never named by its hash, so it
// can never be mistaken for a blob — sweeping it only reclaims space.
func NewStore(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("blobstore: data_dir is empty")
	}
	s := &Store{root: root}
	if err := os.MkdirAll(s.tmpDir(), 0o700); err != nil {
		return nil, fmt.Errorf("creating %s: %w", s.tmpDir(), err)
	}
	if err := s.SweepTemps(); err != nil {
		return nil, err
	}
	return s, nil
}

// Root is the data directory this store owns.
func (s *Store) Root() string { return s.root }

func (s *Store) tmpDir() string { return filepath.Join(s.root, tmpDirName) }

// Path is the absolute path of one blob's bytes. The caller is responsible for
// having validated group, namespace and hash — see validateBlobKey, which every
// caller that takes any of the three from the wire goes through.
func (s *Store) Path(group, namespace, hash string) string {
	return filepath.Join(s.root, group, namespaceDir(namespace), hash)
}

// Has reports whether the promoted file exists.
func (s *Store) Has(group, namespace, hash string) bool {
	st, err := os.Stat(s.Path(group, namespace, hash))
	return err == nil && st.Mode().IsRegular()
}

// Open returns the promoted file for reading. The caller closes it. An *os.File
// is deliberately what comes back rather than an io.Reader: the `/b/` path
// serves Range requests through http.ServeContent, which needs a ReadSeeker.
func (s *Store) Open(group, namespace, hash string) (*os.File, os.FileInfo, error) {
	p := s.Path(group, namespace, hash)
	f, err := os.Open(p)
	if err != nil {
		return nil, nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return f, st, nil
}

// Remove unlinks a promoted file. A missing file is not an error: DeleteBlob is
// idempotent, and the peer may already have been asked to delete it.
func (s *Store) Remove(group, namespace, hash string) error {
	err := os.Remove(s.Path(group, namespace, hash))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// TempFile creates a new partial-upload file. The name carries the upload id
// only as a debugging aid; uniqueness comes from os.CreateTemp's own suffix, so
// two attempts at the same upload cannot collide on one another's temp.
func (s *Store) TempFile(uploadID string) (*os.File, error) {
	return os.CreateTemp(s.tmpDir(), sanitizeTempPrefix(uploadID)+"-")
}

// Promote fsyncs a temp file and renames it into its content-addressed place,
// then fsyncs the containing directory.
//
// THE ORDER IS THE DURABILITY GUARANTEE and all three steps are needed:
//
//   - fsync the FILE, or the rename can land while the bytes are still only in
//     the page cache, and a power loss leaves a correctly-named file with the
//     wrong contents. That is the one failure content addressing cannot detect
//     on its own, because nothing re-hashes a file on read.
//   - rename, which is atomic within a filesystem: a reader sees the old state
//     or the new one, never a half-written blob at the real name.
//   - fsync the DIRECTORY, or the rename itself may not survive a power loss,
//     leaving bytes on disk under a name nothing points at.
//
// An existing file at the destination is DEDUP and is accepted (R9 step 3): the
// bytes are identical by construction — the name is their hash — so the temp is
// simply discarded rather than renamed over a good file. A promoted file is
// never overwritten.
func (s *Store) Promote(f *os.File, group, namespace, hash string) error {
	tmpName := f.Name()
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync %s: %w", tmpName, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", tmpName, err)
	}

	dir := filepath.Dir(s.Path(group, namespace, hash))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}

	if s.Has(group, namespace, hash) {
		// Dedup. Drop the temp; the file that is already there is the same file.
		_ = os.Remove(tmpName)
		return nil
	}

	if err := os.Rename(tmpName, s.Path(group, namespace, hash)); err != nil {
		return fmt.Errorf("promoting %s: %w", tmpName, err)
	}
	return fsyncDir(dir)
}

// DiscardTemp closes and deletes a partial upload. Called on every failure path,
// on both pods (R9 step 3).
func (s *Store) DiscardTemp(f *os.File) {
	if f == nil {
		return
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
}

// SweepTemps deletes every file in the tmp directory.
func (s *Store) SweepTemps() error {
	entries, err := os.ReadDir(s.tmpDir())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("reading %s: %w", s.tmpDir(), err)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(s.tmpDir(), e.Name())); err != nil {
			return fmt.Errorf("removing temp %s: %w", e.Name(), err)
		}
	}
	return nil
}

// BlobKey identifies one stored file. It is the anti-entropy unit: the listing a
// peer serves is a set of these, and a pod copies the ones it lacks.
type BlobKey struct {
	Group     string
	Namespace string
	Hash      string
}

// String is the `<group>/<ns>/<sha256>` form the decision doc specifies — and
// it is exactly Blob.blob_id, so the anti-entropy listing, the on-disk layout
// and the record key are one format that cannot drift.
//
// THE NAMESPACE SEGMENT IS ENCODED, because a namespace is a PATH
// ("/tenants/acme") and the id has to stay three "/"-separated segments to be
// parseable. `/` becomes `:`, which cannot occur in a namespace path (segments
// are letters, digits, `_` and `-`), so the mapping is total and reversible. The
// root namespace "/" is therefore ":" — a real segment rather than an empty one,
// which is what keeps a root-owned blob's id from collapsing to a two-segment
// string.
func (k BlobKey) String() string {
	return k.Group + "/" + namespaceDir(k.Namespace) + "/" + k.Hash
}

// ParseBlobKey reads the `<group>/<ns>/<sha256>` form back, validating every
// segment. It is the ONLY way a group, namespace or hash from the wire becomes a
// path, which is what makes path traversal unrepresentable rather than merely
// unlikely.
func ParseBlobKey(s string) (BlobKey, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 3 {
		return BlobKey{}, fmt.Errorf("blob key %q is not <group>/<namespace>/<hash>", s)
	}
	k := BlobKey{Group: parts[0], Namespace: namespaceFromDir(parts[1]), Hash: parts[2]}
	if err := validateBlobKey(k.Group, k.Namespace, k.Hash); err != nil {
		return BlobKey{}, err
	}
	return k, nil
}

// List walks the data directory and returns every promoted blob it holds.
//
// It is only ever called to answer a peer's anti-entropy listing, which is why
// it is on the peer listener and not the public one: the answer is the complete
// set of hashes this pod holds, for every tenant.
func (s *Store) List() ([]BlobKey, error) {
	var out []BlobKey
	groups, err := os.ReadDir(s.root)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", s.root, err)
	}
	for _, g := range groups {
		if !g.IsDir() || g.Name() == tmpDirName {
			continue
		}
		nsDirs, err := os.ReadDir(filepath.Join(s.root, g.Name()))
		if err != nil {
			return nil, err
		}
		for _, ns := range nsDirs {
			if !ns.IsDir() {
				continue
			}
			files, err := os.ReadDir(filepath.Join(s.root, g.Name(), ns.Name()))
			if err != nil {
				return nil, err
			}
			for _, f := range files {
				if f.IsDir() {
					continue
				}
				out = append(out, BlobKey{
					Group:     g.Name(),
					Namespace: namespaceFromDir(ns.Name()),
					Hash:      f.Name(),
				})
			}
		}
	}
	return out, nil
}

// WriteFrom streams one blob in from a reader and promotes it, verifying the
// hash as it goes. It is the anti-entropy SINK: a pod filling a replaced disk
// from its peer does not trust the peer's naming, it re-hashes the bytes and
// refuses a file whose content does not match the name it was fetched under.
func (s *Store) WriteFrom(k BlobKey, r io.Reader) error {
	f, err := s.TempFile("antientropy")
	if err != nil {
		return err
	}
	h := newHasher()
	if _, err := io.Copy(io.MultiWriter(f, h), r); err != nil {
		s.DiscardTemp(f)
		return err
	}
	if got := h.hex(); got != k.Hash {
		s.DiscardTemp(f)
		return fmt.Errorf("anti-entropy: peer served %s under hash %s", got, k.Hash)
	}
	return s.Promote(f, k.Group, k.Namespace, k.Hash)
}

// fsyncDir fsyncs a directory so a rename into it survives a power loss.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening %s for fsync: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", dir, err)
	}
	return nil
}
