package blobstore

import (
	"strings"
	"testing"
)

// The upload token is the credential that replaces a signed cloud URL (R3), and
// it is raw entropy whose sha256 keys the UploadSession row — the same
// construction as a link token, with no signature and no key (corrected
// 2026-09-27: the HMAC envelope is gone).
func TestUploadTokenIsRandomAndKeyedByItsHash(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 64; i++ {
		token, hash, err := MintUploadToken()
		if err != nil {
			t.Fatalf("MintUploadToken: %v", err)
		}
		if seen[token] {
			t.Fatal("MintUploadToken repeated a token")
		}
		seen[token] = true
		// 32 raw bytes is 43 base64url characters — the doc's ">= 256 random bits".
		if len(token) != 43 {
			t.Fatalf("token is %d characters, want 43 (256 bits, base64url)", len(token))
		}
		if hash != UploadTokenHash(token) {
			t.Fatal("the mint's hash and the PUT path's hash disagree")
		}
		if strings.Contains(hash, token) {
			t.Fatal("the stored hash contains the raw token")
		}
	}
}

// A link token is raw entropy and its hash is the record key (R7): the token must
// not be recoverable from anything stored.
func TestLinkTokenIsRandomAndKeyedByItsHash(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 64; i++ {
		token, hash, err := MintLinkToken()
		if err != nil {
			t.Fatalf("MintLinkToken: %v", err)
		}
		if seen[token] {
			t.Fatal("MintLinkToken repeated a token")
		}
		seen[token] = true
		// 32 raw bytes is 43 base64url characters — the doc's ">= 256 random bits".
		if len(token) != 43 {
			t.Fatalf("token is %d characters, want 43 (256 bits, base64url)", len(token))
		}
		if hash != LinkTokenHash(token) {
			t.Fatal("the mint's hash and the resolve's hash disagree")
		}
		if strings.Contains(hash, token) {
			t.Fatal("the stored hash contains the raw token")
		}
	}
}

// The filename is validated once, at mint, and it is the only source of
// Content-Disposition (R8). These are the injections it exists to refuse.
func TestValidateFilenameRefusesWhatWouldBreakTheHeader(t *testing.T) {
	for name, bad := range map[string]string{
		"empty":            "",
		"CR":               "a\rb.pdf",
		"LF":               "a\nb.pdf",
		"header injection": "x.pdf\r\nSet-Cookie: a=b",
		"quote":            `a"b.pdf`,
		"semicolon":        "a;b.pdf",
		"backslash":        `a\b.pdf`,
		"forward slash":    "a/b.pdf",
		"dot":              ".",
		"dotdot":           "..",
		"NUL":              "a\x00b.pdf",
		"too long":         strings.Repeat("a", 256),
	} {
		if err := ValidateFilename(bad); err == nil {
			t.Errorf("%s: %q was accepted", name, bad)
		}
	}
	for _, good := range []string{"report.pdf", "Q3 results (final).pdf", "ünïcode.pdf", "a.b.c.pdf"} {
		if err := ValidateFilename(good); err != nil {
			t.Errorf("%q was refused: %v", good, err)
		}
	}
}

// The non-ASCII fallback must not be able to smuggle bytes past the validation.
func TestContentDispositionEmitsBothParameters(t *testing.T) {
	got := ContentDisposition("ünïcode réport.pdf")
	want := `attachment; filename="_n_code r_port.pdf"; filename*=UTF-8''%C3%BCn%C3%AFcode%20r%C3%A9port.pdf`
	if got != want {
		t.Fatalf("\n got %q\nwant %q", got, want)
	}
	if !strings.HasPrefix(got, "attachment;") {
		t.Fatal("the disposition must always be attachment")
	}
}

// Magic bytes are checked against the DECLARED type at finish (R4).
func TestCheckMagicBytes(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		head        []byte
		wantErr     bool
	}{
		{"a real pdf", "application/pdf", []byte("%PDF-1.7\n..."), false},
		{"an ELF calling itself a pdf", "application/pdf", []byte{0x7F, 'E', 'L', 'F', 1}, true},
		{"an empty pdf", "application/pdf", nil, true},
		{"a truncated signature", "application/pdf", []byte("%PD"), true},
		{"a real jpeg", "image/jpeg", []byte{0xFF, 0xD8, 0xFF, 0xE0}, false},
		{"a png calling itself a jpeg", "image/jpeg", []byte{0x89, 'P', 'N', 'G'}, true},
		{"a real png", "image/png", []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}, false},
		{"heic, brand at offset 8 unchecked", "image/heic", []byte("\x00\x00\x00\x18ftypheic"), false},
		{"a docx (zip container)", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", []byte("PK\x03\x04"), false},
		{"an ELF calling itself a docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", []byte{0x7F, 'E', 'L', 'F'}, true},
		// A type with no signature to check is accepted on the declared type
		// alone. That limit is deliberate and is stated in magicByTypes.
		{"text/plain has no signature", "text/plain", []byte("hello"), false},
		{"parameters are ignored", "application/pdf; charset=binary", []byte("%PDF-1.4"), false},
	}
	for _, c := range cases {
		err := CheckMagicBytes(c.contentType, c.head)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err=%v, wantErr=%v", c.name, err, c.wantErr)
		}
	}
}

// A group's rules refuse before a byte moves (R4).
func TestGroupRulesRefuseBeforeAnyByteMoves(t *testing.T) {
	groups, err := NewGroups(testConfig().GetVolumeGroups())
	if err != nil {
		t.Fatalf("NewGroups: %v", err)
	}
	if _, err := groups.CheckDeclared("photos", "application/pdf", 10); err == nil {
		t.Error("an unconfigured group was accepted — 'no group, no upload'")
	}
	if _, err := groups.CheckDeclared(testGroup, "application/x-msdownload", 10); err == nil {
		t.Error("a content type the group does not list was accepted")
	}
	if _, err := groups.CheckDeclared(testGroup, "application/pdf", 4096); err == nil {
		t.Error("a declared size past max_blob_bytes was accepted")
	}
	if _, err := groups.CheckDeclared(testGroup, "APPLICATION/PDF; charset=x", 10); err != nil {
		t.Errorf("a media type differing only in case and parameters was refused: %v", err)
	}
}

// An empty group table is a legal state, not an error: it means no upload is
// possible, which is what "no group, no upload" comes to on a cluster whose
// services have declared none.
func TestNoGroupsConfiguredIsLegalAndRefusesEverything(t *testing.T) {
	groups, err := NewGroups(nil)
	if err != nil {
		t.Fatalf("an empty group table must be legal: %v", err)
	}
	if groups.Len() != 0 {
		t.Fatalf("Len = %d, want 0", groups.Len())
	}
	if _, err := groups.Lookup("documents"); err == nil {
		t.Error("a lookup against an empty table succeeded")
	}
}

func TestDuplicateGroupNamesAreRefused(t *testing.T) {
	cfg := testConfig().GetVolumeGroups()
	if _, err := NewGroups(append(cfg, cfg[0])); err == nil {
		t.Fatal("a duplicate group name was accepted; one service's rules would apply to another's blobs")
	}
}

// The blob key is the one place a wire string becomes a path.
func TestBlobKeyRoundTripsAndRefusesTraversal(t *testing.T) {
	for _, ns := range []string{"/", "/tenants/acme", "/tenants/acme/team-a"} {
		k := BlobKey{Group: "documents", Namespace: ns, Hash: strings.Repeat("a", 64)}
		back, err := ParseBlobKey(k.String())
		if err != nil {
			t.Fatalf("ParseBlobKey(%q): %v", k.String(), err)
		}
		if back != k {
			t.Fatalf("round trip: got %+v, want %+v", back, k)
		}
		if n := strings.Count(k.String(), "/"); n != 2 {
			t.Fatalf("blob_id %q has %d separators, want the three-segment form", k.String(), n)
		}
	}
	for name, bad := range map[string]string{
		"traversal group":     "../../etc/:tenants:acme/" + strings.Repeat("a", 64),
		"traversal namespace": "documents/../../" + strings.Repeat("a", 64),
		"short hash":          "documents/:tenants:acme/abc",
		"uppercase hash":      "documents/:tenants:acme/" + strings.Repeat("A", 64),
		"non-hex hash":        "documents/:tenants:acme/" + strings.Repeat("z", 64),
		"too few segments":    "documents/" + strings.Repeat("a", 64),
		"relative namespace":  "documents/tenants:acme/" + strings.Repeat("a", 64),
	} {
		if _, err := ParseBlobKey(bad); err == nil {
			t.Errorf("%s: %q was accepted", name, bad)
		}
	}
}

// NewMirror refuses anything that is not a pair, because "stream to my peer" and
// "stream to all my peers" are different protocols (R2).
func TestNewMirrorRequiresExactlyOnePeerAndToSeeItself(t *testing.T) {
	if _, err := NewMirror([]string{"blobstore-1.blobstore:8081"}, "blobstore-0", nil); err == nil {
		t.Error("a peer list that does not include this pod was accepted")
	}
	if _, err := NewMirror([]string{"blobstore-0.blobstore:8081"}, "blobstore-0", nil); err == nil {
		t.Error("a peer list with no peer at all was accepted")
	}
	if _, err := NewMirror([]string{
		"blobstore-0.blobstore:8081", "blobstore-1.blobstore:8081", "blobstore-2.blobstore:8081",
	}, "blobstore-0", nil); err == nil {
		t.Error("a three-replica peer list was accepted; the mirror is a pair")
	}
	m, err := NewMirror([]string{"blobstore-0.blobstore:8081", "blobstore-1.blobstore:8081"}, "blobstore-0", nil)
	if err != nil {
		t.Fatalf("a well-formed pair was refused: %v", err)
	}
	if m.Peer() != "blobstore-1.blobstore:8081" {
		t.Fatalf("Peer() = %q, want the other replica", m.Peer())
	}
}
