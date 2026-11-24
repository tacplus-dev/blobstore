package main

import (
	"bufio"
	"os"
	"regexp"
	"strings"
	"testing"
)

// The release image's base is pinned by digest, never a floating tag (#25;
// tacplus-dev/roadmap decisions/security-gates-ship-only-2026-11-23.md, point 2):
// every FROM, with its build arguments' defaults substituted, names
// <image>:<tag>@sha256:<digest>, so a rebuild of a commit ships the base it was
// scanned with. Nightly maintenance moves the digest (tacplus-dev/looper#234).
func TestDockerfileFromPinnedByDigest(t *testing.T) {
	f, err := os.Open("Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	pinned := regexp.MustCompile(`^[^\s@]+:[^\s@]+@sha256:[0-9a-f]{64}$`)
	argRef := regexp.MustCompile(`\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?`)
	args := map[string]string{}
	froms := 0
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "ARG":
			name, def, _ := strings.Cut(fields[1], "=")
			args[name] = def
		case "FROM":
			froms++
			image := fields[1]
			if strings.HasPrefix(image, "--") && len(fields) > 2 {
				image = fields[2] // FROM --platform=... <image>
			}
			image = argRef.ReplaceAllStringFunc(image, func(ref string) string {
				return args[argRef.FindStringSubmatch(ref)[1]]
			})
			if !pinned.MatchString(image) {
				t.Errorf("Dockerfile:%d: FROM %q is not pinned by digest (<image>:<tag>@sha256:<digest>)", n, image)
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if froms == 0 {
		t.Fatal("Dockerfile: no FROM")
	}
}
