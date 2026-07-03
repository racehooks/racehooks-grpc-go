package racehooksgrpc_test

import (
	"context"
	"encoding/json"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	racehooksgrpc "github.com/racehooks/racehooks-grpc-go"
	racehooksv1 "github.com/racehooks/racehooks-grpc-go/internal/racehooksv1"
)

// ---------------------------------------------------------------------------
// In-process test server (bufconn) — the Go standard for gRPC client testing.
// ---------------------------------------------------------------------------

type testServer struct {
	racehooksv1.UnimplementedFeedStreamServer
	onSubscribe func(*racehooksv1.SubscribeRequest, racehooksv1.FeedStream_SubscribeServer) error
}

func (s *testServer) Subscribe(req *racehooksv1.SubscribeRequest, stream racehooksv1.FeedStream_SubscribeServer) error {
	return s.onSubscribe(req, stream)
}

// startServer brings up a bufconn-backed gRPC server with the given Subscribe
// handler and returns a Client wired to it over the in-memory connection.
func startServer(t *testing.T, onSubscribe func(*racehooksv1.SubscribeRequest, racehooksv1.FeedStream_SubscribeServer) error, opts ...racehooksgrpc.ClientOption) *racehooksgrpc.Client {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	racehooksv1.RegisterFeedStreamServer(srv, &testServer{onSubscribe: onSubscribe})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	clientOpts := append([]racehooksgrpc.ClientOption{
		racehooksgrpc.WithTLS(false),
		racehooksgrpc.WithDialOptions(
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		),
	}, opts...)

	client, err := racehooksgrpc.NewClient("passthrough:///bufnet", clientOpts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func msg(feed, session string, seq uint64, body any) *racehooksv1.FeedMessage {
	var payload []byte
	if body != nil {
		payload, _ = json.Marshal(body)
	}
	return &racehooksv1.FeedMessage{Feed: feed, SessionId: session, Utc: "t", Seq: seq, JsonPayload: payload}
}

// recv reads one event from the stream or fails after a timeout.
func recv(t *testing.T, s *racehooksgrpc.Stream) racehooksgrpc.FeedEvent {
	t.Helper()
	select {
	case ev, ok := <-s.Messages():
		if !ok {
			t.Fatalf("stream closed early; err=%v", s.Err())
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a message")
		return racehooksgrpc.FeedEvent{}
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestSubscribeDeliversMessages(t *testing.T) {
	client := startServer(t, func(_ *racehooksv1.SubscribeRequest, stream racehooksv1.FeedStream_SubscribeServer) error {
		_ = stream.Send(msg("_connected", "", 0, nil))
		_ = stream.Send(msg("timingdata", "s1", 1, map[string]any{"drivers": []any{map[string]any{"number": "1"}}}))
		<-stream.Context().Done()
		return nil
	})

	stream := client.Subscribe(context.Background(), racehooksgrpc.SubscribeOptions{Feeds: []string{"timingdata"}})
	defer stream.Close()

	ev := recv(t, stream)
	if ev.Feed != "timingdata" || ev.Seq != 1 || ev.SessionID != "s1" {
		t.Fatalf("unexpected event: %+v", ev)
	}
	var got map[string]any
	if err := json.Unmarshal(ev.Payload, &got); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if _, ok := got["drivers"]; !ok {
		t.Fatalf("payload missing drivers: %s", ev.Payload)
	}
}

func TestSubscribeForwardsTokenAndFeeds(t *testing.T) {
	var gotAuth, gotFeeds atomic.Value
	var gotResume atomic.Uint64
	client := startServer(t, func(req *racehooksv1.SubscribeRequest, stream racehooksv1.FeedStream_SubscribeServer) error {
		md, _ := metadata.FromIncomingContext(stream.Context())
		auth := md.Get("authorization")
		if len(auth) > 0 {
			gotAuth.Store(auth[0])
		}
		gotFeeds.Store(req.GetFeeds())
		gotResume.Store(req.GetResumeFrom())
		_ = stream.Send(msg("_connected", "", 0, nil))
		_ = stream.Send(msg("timingdata", "s", 1, map[string]int{"x": 1}))
		<-stream.Context().Done()
		return nil
	}, racehooksgrpc.WithToken("tok-123"))

	stream := client.Subscribe(context.Background(), racehooksgrpc.SubscribeOptions{Feeds: []string{"timingdata", "racecontrol"}})
	defer stream.Close()

	// Receiving a data message proves the server processed the request.
	recv(t, stream)

	if got, _ := gotAuth.Load().(string); got != "Bearer tok-123" {
		t.Fatalf("authorization = %q, want Bearer tok-123", got)
	}
	if got, _ := gotFeeds.Load().([]string); len(got) != 2 || got[0] != "timingdata" || got[1] != "racecontrol" {
		t.Fatalf("feeds = %v", got)
	}
	if gotResume.Load() != 0 {
		t.Fatalf("resumeFrom = %d, want 0", gotResume.Load())
	}
}

func TestSubscribeDedupesBySeq(t *testing.T) {
	client := startServer(t, func(_ *racehooksv1.SubscribeRequest, stream racehooksv1.FeedStream_SubscribeServer) error {
		_ = stream.Send(msg("_connected", "", 0, nil))
		_ = stream.Send(msg("timingdata", "s", 5, map[string]int{"x": 1}))
		_ = stream.Send(msg("timingdata", "s", 5, map[string]int{"x": 1})) // duplicate
		_ = stream.Send(msg("timingdata", "s", 6, map[string]int{"x": 1}))
		<-stream.Context().Done()
		return nil
	})

	stream := client.Subscribe(context.Background(), racehooksgrpc.SubscribeOptions{Feeds: []string{"*"}})
	defer stream.Close()

	if got := recv(t, stream); got.Seq != 5 {
		t.Fatalf("first seq = %d, want 5", got.Seq)
	}
	if got := recv(t, stream); got.Seq != 6 {
		t.Fatalf("second seq = %d, want 6 (duplicate 5 should be dropped)", got.Seq)
	}
}

func TestReconnectResumesFromLastSeq(t *testing.T) {
	var calls atomic.Int32
	var secondResume atomic.Uint64
	client := startServer(t, func(req *racehooksv1.SubscribeRequest, stream racehooksv1.FeedStream_SubscribeServer) error {
		switch calls.Add(1) {
		case 1:
			_ = stream.Send(msg("_connected", "", 0, nil))
			_ = stream.Send(msg("timingdata", "s", 1, map[string]int{"x": 1}))
			return nil // end stream → client should reconnect
		default:
			secondResume.Store(req.GetResumeFrom())
			_ = stream.Send(msg("_connected", "", 0, nil))
			_ = stream.Send(msg("timingdata", "s", 2, map[string]int{"x": 2}))
			<-stream.Context().Done()
			return nil
		}
	})

	stream := client.Subscribe(context.Background(), racehooksgrpc.SubscribeOptions{Feeds: []string{"*"}})
	defer stream.Close()

	if got := recv(t, stream); got.Seq != 1 {
		t.Fatalf("first seq = %d, want 1", got.Seq)
	}
	if got := recv(t, stream); got.Seq != 2 {
		t.Fatalf("second seq = %d, want 2 (after reconnect)", got.Seq)
	}
	if secondResume.Load() != 1 {
		t.Fatalf("reconnect resumeFrom = %d, want 1", secondResume.Load())
	}
}

func TestServerReconnectFrameResubscribes(t *testing.T) {
	var calls atomic.Int32
	client := startServer(t, func(_ *racehooksv1.SubscribeRequest, stream racehooksv1.FeedStream_SubscribeServer) error {
		switch calls.Add(1) {
		case 1:
			_ = stream.Send(msg("_connected", "", 0, nil))
			_ = stream.Send(msg("_reconnect", "", 0, nil)) // server asks for overlap reconnect
			<-stream.Context().Done()
			return nil
		default:
			_ = stream.Send(msg("timingdata", "s", 9, map[string]int{"x": 1}))
			<-stream.Context().Done()
			return nil
		}
	})

	stream := client.Subscribe(context.Background(), racehooksgrpc.SubscribeOptions{Feeds: []string{"*"}})
	defer stream.Close()

	if got := recv(t, stream); got.Seq != 9 {
		t.Fatalf("seq = %d, want 9 (after _reconnect resubscribe)", got.Seq)
	}
	if calls.Load() < 2 {
		t.Fatalf("expected at least 2 Subscribe calls, got %d", calls.Load())
	}
}

func TestDisableReconnectSurfacesTerminalEnd(t *testing.T) {
	client := startServer(t, func(_ *racehooksv1.SubscribeRequest, stream racehooksv1.FeedStream_SubscribeServer) error {
		_ = stream.Send(msg("timingdata", "s", 1, map[string]int{"x": 1}))
		return nil // immediate end
	})

	stream := client.Subscribe(context.Background(), racehooksgrpc.SubscribeOptions{
		Feeds:            []string{"*"},
		DisableReconnect: true,
	})
	defer stream.Close()

	if got := recv(t, stream); got.Seq != 1 {
		t.Fatalf("seq = %d, want 1", got.Seq)
	}
	// With reconnect disabled, the channel closes after the server ends.
	select {
	case _, ok := <-stream.Messages():
		if ok {
			t.Fatal("expected channel to close after terminal end")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel did not close after terminal end")
	}
}

func TestCloseStopsStream(t *testing.T) {
	client := startServer(t, func(_ *racehooksv1.SubscribeRequest, stream racehooksv1.FeedStream_SubscribeServer) error {
		_ = stream.Send(msg("_connected", "", 0, nil))
		_ = stream.Send(msg("timingdata", "s", 1, map[string]int{"x": 1}))
		<-stream.Context().Done()
		return nil
	})

	stream := client.Subscribe(context.Background(), racehooksgrpc.SubscribeOptions{Feeds: []string{"*"}})
	// Receiving the data message proves the stream is live before we Close.
	recv(t, stream)
	stream.Close()

	select {
	case _, ok := <-stream.Messages():
		if ok {
			t.Fatal("expected channel closed after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel did not close after Close")
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("Err after Close = %v, want nil", err)
	}
}
