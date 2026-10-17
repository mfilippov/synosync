//go:build integration

package integration

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// TestIntegration_TwoClientsRapidChurn rewrites one file over and over while the
// engine is trying to push it - a log file, an editor autosaving, a build artifact.
// The upload hashes the bytes and then sends them, so a rewrite landing between the
// two makes the server see a body that does not match the hash it was promised
// (36870), and the file the scan measured is not the file the reader reads.
//
// What must hold once the churn stops: both clients converge on the FINAL content.
// A sync that ends holding an intermediate version - or that keeps retrying a stale
// hash forever - is a defect regardless of how the failure arrived.
func TestIntegration_TwoClientsRapidChurn(t *testing.T) {
	a, b := twoClients(t)

	const rel = "churn.log"
	// Rewrite while the engine is mid-flight. Each round is a different length as well
	// as different bytes, so a torn read cannot compare equal to any whole version.
	deadline := time.Now().Add(15 * time.Second)
	var final []byte
	for i := 0; time.Now().Before(deadline); i++ {
		final = []byte(fmt.Sprintf("churn: revision %03d%s\n", i, string(bytes.Repeat([]byte("x"), i%64))))
		a.writeLocal(t, rel, final)
		a.eng.TriggerSync()
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("wrote the file repeatedly; final revision is %d bytes", len(final))

	waitBoth(t, a, b, twoClientTimeout, func() bool {
		ga, oka := a.readLocal(t, rel)
		gb, okb := b.readLocal(t, rel)
		return oka && okb && bytes.Equal(ga, final) && bytes.Equal(gb, final)
	})
	assertLocalContent(t, a, rel, final)
	if gb, ok := b.readLocal(t, rel); !ok || !bytes.Equal(gb, final) {
		t.Fatalf("client B did not converge on the final revision")
	}
	assertServerContentEventually(t, a, rel, final)
	reportHealth(t, "client A", a)
	reportHealth(t, "client B", b)
	if n := a.eng.StuckOps() + b.eng.StuckOps(); n != 0 {
		t.Fatalf("the churned file left %d stuck op(s)", n)
	}
}

// assertServerContentEventually downloads a scratch-relative server node until its
// bytes equal want, retrying transient refusals and REPORTING them.
//
// It exists because of a measurement this scenario produced: immediately after a burst
// of overwrites, `download` of the freshly-queried node answers errno **32772** - a
// code with no typed sentinel in actions/errors.go, so the client sees it as a generic
// *ServerError. Inbound that means "retry forever" (harmless); outbound it counts
// toward the dead-letter streak. How long the window lasts is what this helper records.
func assertServerContentEventually(t *testing.T, sc *scenario, rel string, want []byte) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		n, ok := sc.queryRel(t, rel)
		if !ok {
			lastErr = fmt.Errorf("server is missing %q", rel)
			time.Sleep(time.Second)
			continue
		}
		ctx, cancel := actionCtx()
		var buf bytes.Buffer
		_, err := sc.client.Download(ctx, n.FileID, sc.view.RootNodeID, 0, &buf)
		cancel()
		if err != nil {
			lastErr = err
			// A rejected turn can leave the client broken by contract; re-dial before retrying.
			sc.redialClient(t)
			time.Sleep(time.Second)
			continue
		}
		if bytes.Equal(buf.Bytes(), want) {
			if attempts > 1 {
				t.Logf("MEASURED: the server refused to serve the just-overwritten node for %d attempt(s) "+
					"(last error %v) before answering correctly", attempts-1, lastErr)
			}
			return
		}
		lastErr = fmt.Errorf("server holds %d bytes, want %d", buf.Len(), len(want))
		time.Sleep(time.Second)
	}
	t.Fatalf("the server never served the final content of %q: %v", rel, lastErr)
}
