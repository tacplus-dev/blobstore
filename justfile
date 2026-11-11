# Local development builds only — not used in CI/CD.

TACPLUS := env_var_or_default("TACPLUS_BIN", "tacplus")

default: generate

generate:
  #!/usr/bin/env bash
  set -euo pipefail
  {{TACPLUS}} build --tag latest

# What the looper (and a person before pushing) runs: regenerate tacplus/ with the
# floor's compiler, check no generated file is tracked (generated code is never
# edited by hand) and no upstream reference crept back, then format, vet, build,
# test the hand-written code against it.
gates: generate
  #!/usr/bin/env bash
  set -euo pipefail
  tracked="$(git ls-files tacplus)"
  test -z "$tracked" || { echo "generated files are tracked:" >&2; echo "$tracked" >&2; exit 1; }
  # tacplus-dev works without its upstream (#3): no reference to it outside the provenance
  # and the dated planning records (the bracket keeps this line from matching itself).
  upstream="$(git grep -n -I -i -E 'sea[b]eam|sona[r]/' -- . ':!PROVENANCE.md' ':!ROADMAP.md' ':!HISTORY.md' ':!STATE.md' ':!PROPOSALS.md' ':!QUESTIONS.md' || true)"
  test -z "$upstream" || { echo "upstream references:" >&2; echo "$upstream" >&2; exit 1; }
  # The release image runs as the fixed non-root UID/GID the operator's securityContext names (#18).
  user="$(grep -E '^USER ' Dockerfile | tail -n1 | awk '{print $2}' || true)"
  test "$user" = "65532:65532" || { echo "Dockerfile: the last USER must be 65532:65532, not '${user:-root}'" >&2; exit 1; }
  unformatted="$(gofmt -l main.go internal)"
  test -z "$unformatted" || { echo "gofmt needed:" >&2; echo "$unformatted" >&2; exit 1; }
  go vet ./...
  go build ./...
  go test ./...
  echo "gates: ok"
