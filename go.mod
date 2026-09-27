module gitlab.com/secondstarcavalier/schematic/platform/services/blobstore

go 1.25.5

// The shared pins are letsencrypt's exactly: both services generate from the
// same proto tag and therefore against the same generated chassis, so a version
// skew between them would be a difference in the generated code's dependencies
// and nothing else.
require (
	github.com/dsnet/compress v0.0.1
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/hashicorp/go-multierror v1.1.1
	github.com/jinzhu/copier v0.4.0
	github.com/pelletier/go-toml/v2 v2.2.4
	go.uber.org/zap v1.27.1
	google.golang.org/grpc v1.79.1
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
)

require golang.org/x/text v0.32.0

require (
	github.com/hashicorp/errwrap v1.0.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
)
