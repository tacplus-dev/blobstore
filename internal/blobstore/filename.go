package blobstore

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

// maxFilenameLen matches BlobLink.filename's (spec.assert_max_size).
const maxFilenameLen = 255

// ValidateFilename is the gate the decision doc puts at mint time (R8): the
// filename is checked ONCE, when the link is created, and stored. `GET /b/` then
// serves what is stored and never takes a filename from the request.
//
// What is refused, and why each one:
//   - CR and LF, and any other control character: these are what turn a
//     Content-Disposition value into a second header. This is the injection the
//     check primarily exists for.
//   - `"` and `\`: both end or escape the quoted-string form of the header value.
//   - `;`: separates header parameters, so a name containing one can append a
//     parameter of the attacker's choosing.
//   - `/` and `\` as path separators, and a name of "." or "..": a download name
//     is a basename. A client that saves "../../x" where its user asked for a
//     file is the client's bug, but it is one this service must not be able to
//     cause.
func ValidateFilename(name string) error {
	if name == "" {
		return fmt.Errorf("filename is empty")
	}
	if len(name) > maxFilenameLen {
		return fmt.Errorf("filename is longer than %d bytes", maxFilenameLen)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("filename %q is a path, not a name", name)
	}
	for _, r := range name {
		switch {
		case r == '/' || r == '\\':
			return fmt.Errorf("filename %q contains a path separator", name)
		case r == '"' || r == ';':
			return fmt.Errorf("filename %q contains %q, which would break the Content-Disposition header", name, r)
		case r == unicode.ReplacementChar:
			return fmt.Errorf("filename %q is not valid UTF-8", name)
		case unicode.IsControl(r):
			return fmt.Errorf("filename %q contains a control character", name)
		}
	}
	return nil
}

// ContentDisposition renders the header for a validated filename, in the exact
// form the decision doc specifies (R8):
//
//	attachment; filename="<ascii-fallback>"; filename*=UTF-8''<pct-encoded>
//
// ALWAYS `attachment`, never `inline`: this service serves other people's bytes
// from an origin that also serves the application, so a blob that renders in the
// browser would be a stored-XSS vector against that origin. A PDF viewer that
// wants to display it fetches it itself.
//
// BOTH parameters are emitted. `filename*` is what every current browser uses and
// carries the real, possibly non-ASCII name; the quoted `filename` is the
// fallback, with every non-ASCII rune replaced by `_` so the fallback cannot
// smuggle bytes the validation above was checking for.
func ContentDisposition(name string) string {
	var ascii strings.Builder
	for _, r := range name {
		if r < 0x20 || r > 0x7E {
			ascii.WriteByte('_')
			continue
		}
		ascii.WriteRune(r)
	}
	// PathEscape, not QueryEscape: a space must become %20, not '+'. RFC 5987's
	// ext-value is percent-encoding, and a '+' there is a literal plus.
	return fmt.Sprintf("attachment; filename=%q; filename*=UTF-8''%s", ascii.String(), url.PathEscape(name))
}
