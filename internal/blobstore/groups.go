package blobstore

import (
	"bytes"
	"fmt"
	"strings"

	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
)

// sniffBytes is how much of the head of the stream is kept for the magic-byte
// check. 512 is enough for every signature in the table below (the deepest is a
// HEIC brand at offset 8) with room for a format that needs more later, and it
// is one small allocation per upload rather than a second pass over the file.
const sniffBytes = 512

// Groups is the volume-group rule table, as delivered in config.
//
// A GROUP IS NEVER CREATED HERE. Groups are declared at build time by the
// consuming service and rendered into config by the operator (R4); this type only
// answers "is there a group by this name, and does it admit this?". An unknown
// group is refused — "no group, no upload" — which is also why an empty table is
// a correct state and not an error: a cluster whose services declare no groups
// accepts no uploads.
type Groups struct {
	byName map[string]*blobstore_pb.VolumeGroup
}

// NewGroups indexes the config's groups by name. A duplicate name is refused
// rather than resolved: tacplus already guarantees uniqueness across the fleet,
// so two entries here mean the rendering is wrong and silently picking one would
// mean one service's rules being applied to another's blobs.
func NewGroups(cfg []*blobstore_pb.VolumeGroup) (*Groups, error) {
	g := &Groups{byName: make(map[string]*blobstore_pb.VolumeGroup, len(cfg))}
	for _, vg := range cfg {
		name := vg.GetName()
		if err := validateGroupName(name); err != nil {
			return nil, fmt.Errorf("volume group: %w", err)
		}
		if _, dup := g.byName[name]; dup {
			return nil, fmt.Errorf("volume group %q is declared twice in config", name)
		}
		g.byName[name] = vg
	}
	return g, nil
}

// Len is how many groups are configured. Zero is legal; see the type comment.
func (g *Groups) Len() int { return len(g.byName) }

// Lookup returns the group's rules, or an error naming the group. The error text
// reaches an owning service over an authenticated RPC, so naming the group is
// diagnostics rather than disclosure.
func (g *Groups) Lookup(name string) (*blobstore_pb.VolumeGroup, error) {
	vg, ok := g.byName[name]
	if !ok {
		return nil, fmt.Errorf("no volume group %q is configured", name)
	}
	return vg, nil
}

// CheckDeclared applies the rules that can be checked before any byte arrives:
// the group exists, the declared type is on its allow-list, and the declared
// size is within max_blob_bytes.
//
// The declared size being within the limit does NOT make the wire check
// redundant — see Upload's own comment. A client that declares 1 KiB and sends
// 1 GiB has to be stopped by the stream, not by this.
func (g *Groups) CheckDeclared(group, contentType string, size int64) (*blobstore_pb.VolumeGroup, error) {
	vg, err := g.Lookup(group)
	if err != nil {
		return nil, err
	}
	if !contentTypeAllowed(vg, contentType) {
		return nil, fmt.Errorf("volume group %q does not allow content type %q", group, contentType)
	}
	if size > vg.GetMaxBlobBytes() {
		return nil, fmt.Errorf("volume group %q allows at most %d bytes, declared %d", group, vg.GetMaxBlobBytes(), size)
	}
	return vg, nil
}

// contentTypeAllowed compares against the group's exhaustive list, ignoring any
// parameters the caller attached (`text/plain; charset=utf-8` matches an entry
// of `text/plain`) and ignoring case, which is what RFC 9110 says a media type
// comparison is.
func contentTypeAllowed(vg *blobstore_pb.VolumeGroup, contentType string) bool {
	want := normalizeContentType(contentType)
	for _, allowed := range vg.GetContentTypes() {
		if normalizeContentType(allowed) == want {
			return true
		}
	}
	return false
}

func normalizeContentType(ct string) string {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.ToLower(strings.TrimSpace(ct))
}

// magicSignature is one accepted byte pattern for a media type.
type magicSignature struct {
	offset int
	prefix []byte
}

// magicByTypes is the magic-byte table (R4): what the head of the stream must
// look like for a declared content type to be believed.
//
// A type ABSENT from this table is accepted on the declared type alone, and that
// is a deliberate, stated limit rather than an oversight: there is no signature
// for `text/plain` or `text/csv` to check, and inventing a heuristic for them
// would reject legitimate files. The check exists for the case the doc names —
// an executable renamed to look like a document — and every container format a
// document could plausibly be is listed.
var magicByTypes = map[string][]magicSignature{
	"application/pdf": {{0, []byte("%PDF-")}},
	"image/jpeg":      {{0, []byte{0xFF, 0xD8, 0xFF}}},
	"image/png":       {{0, []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}}},
	"image/gif":       {{0, []byte("GIF87a")}, {0, []byte("GIF89a")}},
	"image/webp":      {{0, []byte("RIFF")}},
	"image/tiff":      {{0, []byte{0x49, 0x49, 0x2A, 0x00}}, {0, []byte{0x4D, 0x4D, 0x00, 0x2A}}},
	// HEIC and the other ISO-BMFF images carry `ftyp` at offset 4 and their brand
	// at offset 8. Only `ftyp` is checked: the brand set is open (heic, heix,
	// hevc, mif1, …) and a list of them would be a list to keep up to date.
	"image/heic": {{4, []byte("ftyp")}},
	"image/heif": {{4, []byte("ftyp")}},
	// The OOXML office formats are ZIP containers; the legacy ones are OLE2.
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document":   {{0, []byte("PK\x03\x04")}},
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":         {{0, []byte("PK\x03\x04")}},
	"application/vnd.openxmlformats-officedocument.presentationml.presentation": {{0, []byte("PK\x03\x04")}},
	"application/vnd.oasis.opendocument.text":                                   {{0, []byte("PK\x03\x04")}},
	"application/vnd.oasis.opendocument.spreadsheet":                            {{0, []byte("PK\x03\x04")}},
	"application/zip":               {{0, []byte("PK\x03\x04")}},
	"application/msword":            {{0, []byte{0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1}}},
	"application/vnd.ms-excel":      {{0, []byte{0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1}}},
	"application/vnd.ms-powerpoint": {{0, []byte{0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1}}},
}

// CheckMagicBytes verifies the head of the stream against the declared type
// (R4). It runs AT FINISH rather than as soon as the first bytes arrive, because
// a stream shorter than a signature is only knowable to be short once it ends —
// an empty upload declaring `application/pdf` must fail here, not be waited on.
func CheckMagicBytes(contentType string, head []byte) error {
	sigs, known := magicByTypes[normalizeContentType(contentType)]
	if !known {
		return nil
	}
	for _, sig := range sigs {
		end := sig.offset + len(sig.prefix)
		if len(head) >= end && bytes.Equal(head[sig.offset:end], sig.prefix) {
			return nil
		}
	}
	return fmt.Errorf("content does not look like %s", normalizeContentType(contentType))
}
