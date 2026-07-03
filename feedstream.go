// Package racehooksgrpc is the RaceHooks gRPC streaming client for Go.
//
// It delivers F1 feed messages over a gRPC server-stream with automatic,
// lossless reconnect. Most integrations should use the REST + webhook SDK
// (github.com/racehooks/racehooks-go); reach for this package when you want a
// single multiplexed gRPC socket — typically high-throughput or latency-sensitive
// back ends. Requires the Analytics tier.
//
// # Quick start
//
//	client, err := racehooksgrpc.NewClient("grpc-gateway-xxxx.run.app:443",
//	    racehooksgrpc.WithToken(os.Getenv("RACEHOOKS_TOKEN")))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	stream := client.Subscribe(ctx, racehooksgrpc.SubscribeOptions{
//	    Feeds: []string{"timingdata", "racecontrol"},
//	})
//	defer stream.Close()
//
//	for ev := range stream.Messages() {
//	    log.Printf("%s seq=%d %s", ev.Feed, ev.Seq, ev.Payload)
//	}
//	if err := stream.Err(); err != nil {
//	    log.Fatal(err)
//	}
//
// # Delivery semantics
//
// The stream is best-effort and low-latency; webhooks remain the strict
// at-least-once durable path. Reconnects are handled automatically: the server
// closes long streams with a reconnect hint (~55 min Cloud Run cap), and
// transient drops are retried with exponential backoff. Both resume from the last
// seen sequence, so the gateway replays the gap and duplicates are dropped by
// seq — no messages are lost across a reconnect on a live instance.
package racehooksgrpc

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	racehooksv1 "github.com/racehooks/racehooks-grpc-go/internal/racehooksv1"
)

const (
	controlConnected = "_connected"
	controlReconnect = "_reconnect"

	defaultMaxBackoff = 30 * time.Second
	initialBackoff    = 1 * time.Second
)

// Client is a connection to a RaceHooks grpc-gateway. Create one with NewClient
// and reuse it for the process lifetime; it is safe for concurrent use.
type Client struct {
	conn  *grpc.ClientConn
	raw   racehooksv1.FeedStreamClient
	token string
}

// ClientOption configures a Client.
type ClientOption func(*clientConfig)

type clientConfig struct {
	token       string
	tls         bool
	dialOptions []grpc.DialOption
}

// WithToken sets the Bearer token sent as gRPC authorization metadata on every
// subscription. Obtain it from POST /v1/oauth.
func WithToken(token string) ClientOption {
	return func(c *clientConfig) { c.token = token }
}

// WithTLS toggles transport security. Defaults to true (Cloud Run). Set false
// only for a local h2c gateway or in tests.
func WithTLS(enabled bool) ClientOption {
	return func(c *clientConfig) { c.tls = enabled }
}

// WithDialOptions appends raw gRPC dial options — an escape hatch for advanced
// transport tuning (keepalive, interceptors, custom credentials).
func WithDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(c *clientConfig) { c.dialOptions = append(c.dialOptions, opts...) }
}

// NewClient dials the grpc-gateway at target. target may be "host:443",
// "host:port", or a full https URL (the scheme and path are stripped).
func NewClient(target string, opts ...ClientOption) (*Client, error) {
	cfg := &clientConfig{tls: true}
	for _, o := range opts {
		o(cfg)
	}

	dialOpts := make([]grpc.DialOption, 0, len(cfg.dialOptions)+1)
	if cfg.tls {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(nil)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, cfg.dialOptions...)

	conn, err := grpc.NewClient(normalizeTarget(target), dialOpts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, raw: racehooksv1.NewFeedStreamClient(conn), token: cfg.token}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// normalizeTarget accepts a full https URL or a bare host[:port] and returns a
// gRPC dial target host:port (defaulting to :443).
func normalizeTarget(target string) string {
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		if u, err := url.Parse(target); err == nil {
			host := u.Hostname()
			port := u.Port()
			if port == "" {
				if u.Scheme == "http" {
					port = "80"
				} else {
					port = "443"
				}
			}
			return host + ":" + port
		}
	}
	if !strings.Contains(target, ":") {
		return target + ":443"
	}
	return target
}

// Filters narrows delivery, with the same semantics as webhook/SSE filters.
type Filters struct {
	DriverNumbers []int32
	Constructors  []string
	EventTypes    []string
}

// SubscribeOptions configures a subscription.
type SubscribeOptions struct {
	// Feeds to receive, or []string{"*"} for every feed your tier allows.
	Feeds []string
	// Filters is optional per-connection filtering.
	Filters *Filters
	// DisableReconnect stops automatic reconnect; the stream then ends (and Err
	// is set) on the first transport error. Zero value keeps reconnect enabled.
	DisableReconnect bool
	// MaxBackoff caps the delay between reconnect attempts. Zero uses 30s.
	MaxBackoff time.Duration
}

// FeedEvent is one delivered feed message. Payload is the raw JSON body — the
// same shape as the webhook/SSE payload — left for the caller to unmarshal.
type FeedEvent struct {
	Feed      string
	SessionID string
	UTC       string
	Seq       uint64
	Payload   json.RawMessage
}

// Subscribe opens a feed subscription. It returns immediately; messages arrive on
// Messages(). Cancel by calling Stream.Close or cancelling ctx.
func (c *Client) Subscribe(ctx context.Context, opts SubscribeOptions) *Stream {
	ctx, cancel := context.WithCancel(ctx)
	maxBackoff := opts.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxBackoff
	}
	s := &Stream{
		client:     c,
		opts:       opts,
		maxBackoff: maxBackoff,
		ctx:        ctx,
		cancel:     cancel,
		msgs:       make(chan FeedEvent, 64),
	}
	go s.run()
	return s
}

// Stream is a live subscription. Range over Messages until it is closed, then
// check Err. A Stream is not safe for concurrent Close from multiple goroutines
// beyond the idempotency guarantee.
type Stream struct {
	client     *Client
	opts       SubscribeOptions
	maxBackoff time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	msgs   chan FeedEvent

	maxSeq    uint64
	closeOnce sync.Once

	mu  sync.Mutex
	err error
}

// Messages returns the channel of feed events. It is closed when the stream ends
// (after Close, ctx cancellation, or a terminal error when reconnect is disabled).
func (s *Stream) Messages() <-chan FeedEvent { return s.msgs }

// Err returns the terminal error after Messages is closed, or nil if the stream
// ended via Close or context cancellation.
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close stops the subscription. Idempotent.
func (s *Stream) Close() {
	s.closeOnce.Do(func() { s.cancel() })
}

func (s *Stream) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *Stream) request() *racehooksv1.SubscribeRequest {
	req := &racehooksv1.SubscribeRequest{Feeds: s.opts.Feeds, ResumeFrom: s.maxSeq}
	if f := s.opts.Filters; f != nil {
		req.Filters = &racehooksv1.Filters{
			DriverNumbers: f.DriverNumbers,
			Constructors:  f.Constructors,
			EventTypes:    f.EventTypes,
		}
	}
	return req
}

func (s *Stream) authContext() context.Context {
	if s.client.token == "" {
		return s.ctx
	}
	return metadata.AppendToOutgoingContext(s.ctx, "authorization", "Bearer "+s.client.token)
}

// run owns the reconnect loop and is the only writer to msgs.
func (s *Stream) run() {
	defer close(s.msgs)

	attempt := 0
	for {
		if s.ctx.Err() != nil {
			return // Close() or parent ctx cancelled
		}

		stream, err := s.client.raw.Subscribe(s.authContext(), s.request())
		if err != nil {
			if !s.retry(&attempt, err) {
				return
			}
			continue
		}

		// Read until the stream ends, then decide whether to reconnect.
		reconnect, recvErr := s.read(stream)
		if s.ctx.Err() != nil {
			return
		}
		if reconnect {
			// Server reconnect hint — resume immediately (no backoff); the
			// gateway replays seq > ResumeFrom and duplicates are dropped.
			attempt = 0
			continue
		}
		if !s.retry(&attempt, recvErr) {
			return
		}
	}
}

// read consumes one server stream. It returns (reconnect=true) when the server
// asked us to reconnect, otherwise the receive error that ended the stream.
func (s *Stream) read(stream racehooksv1.FeedStream_SubscribeClient) (reconnect bool, err error) {
	for {
		msg, recvErr := stream.Recv()
		if recvErr != nil {
			return false, recvErr
		}
		switch msg.GetFeed() {
		case controlConnected:
			// handshake — nothing to emit
		case controlReconnect:
			return true, nil
		default:
			if !s.emit(msg) {
				return false, s.ctx.Err()
			}
		}
	}
}

// emit delivers a data message, applying seq dedupe. Returns false if the stream
// is shutting down.
func (s *Stream) emit(msg *racehooksv1.FeedMessage) bool {
	seq := msg.GetSeq()
	if seq > 0 && seq <= s.maxSeq {
		return true // duplicate from a resume overlap
	}
	if seq > s.maxSeq {
		s.maxSeq = seq
	}
	ev := FeedEvent{
		Feed:      msg.GetFeed(),
		SessionID: msg.GetSessionId(),
		UTC:       msg.GetUtc(),
		Seq:       seq,
		Payload:   json.RawMessage(msg.GetJsonPayload()),
	}
	select {
	case s.msgs <- ev:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// retry sleeps with exponential backoff and reports whether to keep going.
// It records a terminal error and stops when reconnect is disabled or ctx ends.
func (s *Stream) retry(attempt *int, cause error) bool {
	if s.ctx.Err() != nil {
		return false
	}
	// A clean server-side end (EOF) with reconnect disabled is not an error.
	if s.opts.DisableReconnect {
		if cause != nil && status.Code(cause) != codes.Canceled {
			s.setErr(cause)
		}
		return false
	}

	delay := initialBackoff << min(*attempt, 5) // 1s,2s,…capped by shift then max
	if delay > s.maxBackoff {
		delay = s.maxBackoff
	}
	*attempt++

	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-s.ctx.Done():
		return false
	}
}
