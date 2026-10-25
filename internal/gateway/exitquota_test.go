package gateway

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func quotaResp() *http.Response {
	return &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(bytes.NewReader(
		[]byte(`{"type":"error","error":{"type":"FreeUsageLimitError","message":"Free usage limit reached."}}`)))}
}

func TestQuotaStrikesCoolUntilUTCMidnight(t *testing.T) {
	static := &proxyTransport{name: "http://static"}
	pool := &proxyTransport{name: "http://pool", pool: true}
	p := &anonymousNodes{cooldown: 15 * time.Second, quotaStrikes: 2,
		nodes: []*anonymousNode{{proxy: static}, {proxy: pool}}}
	n := p.nodes[0]

	if !peekFreeUsageLimit(quotaResp()) || peekFreeUsageLimit(&http.Response{StatusCode: 429, Body: io.NopCloser(bytes.NewReader([]byte(`rate limited`)))}) {
		t.Fatal("only FreeUsageLimit 429s are quota")
	}
	if p.MarkQuotaExhausted(n, false) || time.Until(time.Unix(0, n.cooldownUntil.Load())) > 2*time.Minute {
		t.Fatal("one stray user 429 only gets the short failure cooldown, not a whole day")
	}
	if !p.MarkQuotaExhausted(n, false) {
		t.Fatal("the second consecutive 429 must cool the exit")
	}
	left := time.Until(time.Unix(0, n.cooldownUntil.Load()))
	if left <= 0 || left > 24*time.Hour+time.Minute {
		t.Fatalf("cooldown must end at the next UTC midnight, got %s", left)
	}
	p.MarkSuccess(n)
	if n.cooldownUntil.Load() != 0 || n.quotaStrikes.Load() != 0 {
		t.Fatal("a success clears the quota state")
	}
	if !p.MarkQuotaExhausted(n, true) {
		t.Fatal("a probe 429 cools at once")
	}
	if p.MarkQuotaExhausted(p.nodes[1], true) {
		t.Fatal("#pool exits draw a fresh IP and are never cooled")
	}

	// The cooldown survives a restart.
	g := &Gateway{anonymous: p, logger: slog.Default(), exitQuotaPath: filepath.Join(t.TempDir(), "q.json")}
	g.writeExitQuota(time.Now())
	want := n.cooldownUntil.Load()
	n.cooldownUntil.Store(0)
	n.quotaStrikes.Store(0)
	g.loadExitQuota()
	if n.cooldownUntil.Load() != want {
		t.Fatal("persisted cooldown was not restored")
	}
	if g.readExitQuota().ProbedAt.IsZero() {
		t.Fatal("probe time must be persisted so a restart does not probe again")
	}
}

func TestCursorPrefersVerifiedExits(t *testing.T) {
	mk := func(name string) *anonymousNode {
		n := &anonymousNode{proxy: &proxyTransport{name: name}}
		n.proxy.healthy.Store(true)
		return n
	}
	a, b, c := mk("a"), mk("b"), mk("c")
	p := &anonymousNodes{cooldown: 15 * time.Second, quotaStrikes: 2, nodes: []*anonymousNode{a, b, c}}
	p.MarkSuccess(c) // only c is known good
	cur := anonymousCursor{pool: p}
	if got := cur.Next(); got != c {
		t.Fatalf("verified exit must come first, got %s", got.proxy.name)
	}
	if got := cur.Next(); got == c || got == nil {
		t.Fatal("then the rest, each once")
	}
	p.MarkQuotaExhausted(c, false)
	if c.verified.Load() {
		t.Fatal("a quota 429 clears verified")
	}
}
