# racehooks-grpc-go

[![Go Reference](https://pkg.go.dev/badge/github.com/racehooks/racehooks-grpc-go.svg)](https://pkg.go.dev/github.com/racehooks/racehooks-grpc-go)

Low-latency RaceHooks F1 feed delivery over a **gRPC server-stream**, with automatic,
lossless reconnect. Go module — import path `github.com/racehooks/racehooks-grpc-go`,
package `racehooksgrpc`.

Most integrations should use [`racehooks-go`](https://github.com/racehooks/racehooks-go)
(REST + webhooks, no dependencies). Reach for this package when you want a single
multiplexed gRPC socket — typically high-throughput or latency-sensitive back ends.
Requires the **Analytics** tier.

```bash
go get github.com/racehooks/racehooks-grpc-go
```

```go
package main

import (
	"context"
	"log"
	"os"

	racehooksgrpc "github.com/racehooks/racehooks-grpc-go"
)

func main() {
	client, err := racehooksgrpc.NewClient("grpc-gateway-xxxx.run.app:443",
		racehooksgrpc.WithToken(os.Getenv("RACEHOOKS_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	stream := client.Subscribe(context.Background(), racehooksgrpc.SubscribeOptions{
		Feeds:   []string{"timingdata", "racecontrol"},
		Filters: &racehooksgrpc.Filters{DriverNumbers: []int32{1, 16}},
	})
	defer stream.Close()

	for ev := range stream.Messages() {
		log.Printf("%s seq=%d %s", ev.Feed, ev.Seq, ev.Payload) // Payload is raw JSON
	}
	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}
}
```

## Delivery semantics

The stream is **best-effort, low-latency**; webhooks remain the strict at-least-once
durable path. Reconnects are handled for you and are **lossless on a live instance**:

- The server closes long streams with a reconnect hint (~55 min Cloud Run cap); the
  client resubscribes immediately.
- Transient drops retry with exponential backoff.
- Both resume with `ResumeFrom = lastSeq`, so the gateway replays the gap and duplicates
  are dropped by `Seq` — no messages lost across a reconnect.

Set `SubscribeOptions.DisableReconnect` to surface terminal errors via `Stream.Err()`
instead. Run a webhook alongside the stream if you need guaranteed delivery across an
instance recycle.

## Development

Generated stubs (`internal/racehooksv1`) are committed. Regenerate with
[`buf`](https://buf.build) after changing the proto:

```bash
make generate   # buf generate
make test       # go test -race
make lint       # buf lint + go vet + gofmt check
```

The protobuf contract (`proto/racehooks/v1/feed.proto`) is shared across languages and
must stay in sync with the backend gateway and the other clients.

## Documentation

Full docs: https://racehooks.io/docs/sdk

## License

MIT
