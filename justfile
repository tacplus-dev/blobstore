# Local development builds only — not used in CI/CD.

TACPLUS := env_var_or_default("TACPLUS_BIN", "tacplus")

default: generate

generate:
  #!/usr/bin/env bash
  set -euo pipefail
  {{TACPLUS}} build --tag latest

# What the looper (and a person before pushing) runs: regenerate model/ with the
# floor's compiler, check no generated file is tracked (generated code is never
# edited by hand), then format, vet, build, test the hand-written code against it.
gates: generate
  #!/usr/bin/env bash
  set -euo pipefail
  tracked="$(git ls-files model)"
  test -z "$tracked" || { echo "generated files are tracked:" >&2; echo "$tracked" >&2; exit 1; }
  unformatted="$(gofmt -l main.go internal)"
  test -z "$unformatted" || { echo "gofmt needed:" >&2; echo "$unformatted" >&2; exit 1; }
  go vet ./...
  go build ./...
  go test ./...
  echo "gates: ok"
