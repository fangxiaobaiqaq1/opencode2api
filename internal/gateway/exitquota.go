package gateway

// Per-exit daily free quota (fork addition).
//
// OpenCode counts the anonymous free allowance per egress IP per UTC day and
// resets it at UTC midnight (console ipRateLimiter: key ip+YYYYMMDD, retryAfter
// = time to midnight). Upstream opencode2api cools a 429 exit for seconds; with
// performance.quota_strikes set, an exhausted static exit is instead cooled
// until the next UTC midnight (see anonymousNodes.MarkQuotaExhausted), and with
// performance.exit_probe_hours set, every static exit gets one real minimal
// request on that interval so the quota state is measured instead of guessed.
// Both survive restarts and config reloads through <config>.exitquota.json.

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	wire "opencode2api/internal/protocol"
)

// exitProbeModel is the free model the exit probe spends one request on.
const exitProbeModel = "glm-5.3-flash-free"

// exitProbeConcurrency bounds the parallel exit probes.
const exitProbeConcurrency = 8

type exitQuotaFile struct {
	ProbedAt time.Time            `json:"probed_at"`
	Cooled   map[string]time.Time `json:"cooled"`   // proxy name -> cooled until
	Verified []string             `json:"verified"` // exits whose last answer was a 2xx
}

var exitQuotaMu sync.Mutex

// loadExitQuota restores the quota cooldowns that have not expired yet.
func (g *Gateway) loadExitQuota() {
	f := g.readExitQuota()
	now := time.Now()
	verified := map[string]bool{}
	for _, name := range f.Verified {
		verified[name] = true
	}
	for _, node := range g.anonymous.nodes {
		node.verified.Store(verified[node.proxy.name])
		if until, ok := f.Cooled[node.proxy.name]; ok && until.After(now) {
			node.cooldownUntil.Store(until.UnixNano())
			node.quotaStrikes.Store(int32(max(g.anonymous.quotaStrikes, 1)))
		}
	}
}

func (g *Gateway) readExitQuota() exitQuotaFile {
	f := exitQuotaFile{Cooled: map[string]time.Time{}}
	if g.exitQuotaPath == "" {
		return f
	}
	exitQuotaMu.Lock()
	defer exitQuotaMu.Unlock()
	if b, err := os.ReadFile(g.exitQuotaPath); err == nil {
		_ = json.Unmarshal(b, &f)
	}
	if f.Cooled == nil {
		f.Cooled = map[string]time.Time{}
	}
	return f
}

// saveExitQuota writes the live quota cooldowns (strikes at the threshold and
// a cooldown still running) and keeps the recorded probe time.
func (g *Gateway) saveExitQuota() {
	g.writeExitQuota(time.Time{})
}

func (g *Gateway) writeExitQuota(probedAt time.Time) {
	if g.exitQuotaPath == "" {
		return
	}
	f := g.readExitQuota()
	if !probedAt.IsZero() {
		f.ProbedAt = probedAt
	}
	f.Cooled = map[string]time.Time{}
	f.Verified = f.Verified[:0]
	now := time.Now().UnixNano()
	threshold := int32(max(g.anonymous.quotaStrikes, 1))
	for _, node := range g.anonymous.nodes {
		if until := node.cooldownUntil.Load(); until > now && node.quotaStrikes.Load() >= threshold {
			f.Cooled[node.proxy.name] = time.Unix(0, until)
		}
		if node.verified.Load() {
			f.Verified = append(f.Verified, node.proxy.name)
		}
	}
	b, err := json.MarshalIndent(f, "", " ")
	if err != nil {
		return
	}
	exitQuotaMu.Lock()
	defer exitQuotaMu.Unlock()
	tmp := g.exitQuotaPath + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		_ = os.Rename(tmp, g.exitQuotaPath)
	}
}

// StartExitProbes runs probeExits every performance.exit_probe_hours. The
// first run waits out the rest of the interval since the persisted last probe,
// so a restart does not spend a round of quota again.
func (g *Gateway) StartExitProbes(ctx context.Context) {
	hours := g.cfg.Performance.ExitProbeHours
	if hours <= 0 || !g.cfg.Anonymous || g.anonymous.Len() == 0 || g.warp != nil {
		return
	}
	interval := time.Duration(hours) * time.Hour
	wait := max(time.Until(g.readExitQuota().ProbedAt.Add(interval)), 0)
	go func() {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				g.probeExits(ctx)
				timer.Reset(interval)
			}
		}
	}()
}

// probeExits spends one real minimal request through every static anonymous
// exit (the anonymous upstreams in their normal order, stopping at the first
// success): a success clears the exit, a FreeUsageLimit 429 cools it until UTC
// midnight at once (a clean single probe is conclusive), anything else leaves
// the state alone. #pool entries draw a new IP per request and are skipped,
// as are exits under the free tier's abuse block.
func (g *Gateway) probeExits(ctx context.Context) {
	started := time.Now()
	upstreams := g.anonymousUpstreams()
	var mu sync.Mutex
	var wg sync.WaitGroup
	counts := map[string]int{}
	sem := make(chan struct{}, exitProbeConcurrency)
	for _, node := range g.anonymous.nodes {
		if node.proxy == nil || node.proxy.pool || node.proxy.blocked(time.Now()) {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(node *anonymousNode) {
			defer wg.Done()
			defer func() { <-sem }()
			ids := identity.RequestIDs{Session: identity.CanonicalSessionID("exit-probe:" + node.proxy.name), Project: "global"}
			outcome := "inconclusive"
			for _, baseURL := range upstreams {
				if ctx.Err() != nil {
					return
				}
				result := g.probeAttempt(ctx, g.inferenceClient(node.proxy, baseURL, exitProbeModel), baseURL, anonymousZenKey, exitProbeModel, wire.Chat, ids, "", false)
				if result.success {
					g.anonymous.MarkSuccess(node)
					outcome = "ok"
					break
				}
				if result.quota {
					g.anonymous.MarkQuotaExhausted(node, true)
					outcome = "quota"
				}
			}
			mu.Lock()
			counts[outcome]++
			mu.Unlock()
		}(node)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return
	}
	g.writeExitQuota(time.Now())
	g.logger.Info("exit quota probe finished", "component", "upstream", "event", "exit_probe_finished",
		"ok", counts["ok"], "quota_exhausted", counts["quota"], "inconclusive", counts["inconclusive"],
		"cooled_until", time.Now().Add(utcMidnightDelay(time.Now())).Local().Format("01-02 15:04"),
		"duration", time.Since(started).Round(time.Second).String(), "upstreams", config.RedactURL(upstreams[0]))
}
