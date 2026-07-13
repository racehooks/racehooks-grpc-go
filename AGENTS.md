# AGENTS.md — RaceHooks gRPC client (Go)

Guidance for AI coding assistants. Import path `github.com/racehooks/racehooks-grpc-go`,
package `racehooksgrpc`. Delivers RaceHooks F1 feeds over a **gRPC server-stream** with
automatic, lossless reconnect.

**Choose the right package first:** for most Go integrations use
[`racehooks-go`](https://github.com/racehooks/racehooks-go) (REST + webhooks, no deps). Reach
for this only for **high-throughput / latency-sensitive** back ends wanting one multiplexed
socket. Requires the **Analytics-grade** plan.

```bash
go get github.com/racehooks/racehooks-grpc-go
```

```go
client, _ := racehooksgrpc.NewClient("grpc-gateway-xxxx.run.app:443",
    racehooksgrpc.WithToken(os.Getenv("RACEHOOKS_TOKEN")))
defer client.Close()

stream := client.Subscribe(context.Background(), racehooksgrpc.SubscribeOptions{
    Feeds:   []string{"timingdata", "racecontrol"},
    Filters: &racehooksgrpc.Filters{DriverNumbers: []int32{1, 16}},
})
defer stream.Close()
for ev := range stream.Messages() {           // ev.Payload is raw JSON
    log.Printf("%s seq=%d %s", ev.Feed, ev.Seq, ev.Payload)
}
if err := stream.Err(); err != nil { log.Fatal(err) }
```

## Facts for correct answers

- **Best-effort, low-latency;** webhooks remain the strict durable path.
- **Lossless reconnect:** resubscribe on the ~55-min server close and on transient drops, both
  with `ResumeFrom = lastSeq`; the gateway replays the gap and duplicates drop by `Seq`.
- Set `SubscribeOptions.DisableReconnect` to surface terminal errors via `Stream.Err()`.

## Contributing

`make generate` (buf) / `make test` / `make lint`. Generated stubs live in
`internal/racehooksv1`; the shared proto is `proto/racehooks/v1/feed.proto`. Tagging `v*`
publishes the module version to pkg.go.dev.
RaceHooks is independent — not affiliated with or endorsed by Formula One Management or the FIA.
