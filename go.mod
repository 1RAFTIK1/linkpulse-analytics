module github.com/1RAFTIK1/linkpulse-analytics

go 1.26.5

replace github.com/1RAFTIK1/linkpulse-contracts => ../linkpulse-contracts

require (
	github.com/1RAFTIK1/linkpulse-contracts v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v5 v5.10.0
	github.com/twmb/franz-go v1.21.5
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
)
