//go:build integration

package integration

import (
	"bytes"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
)

// lfTimeout bounds how long a single large-file round-trip may take before the
// scenario fails. It is more generous than the small-file scenarios because a
// multi-megabyte chunked upload/download over a slow stand legitimately takes
// longer than a one-frame transfer.
//
// It must also leave room for ONE recovery. A download whose peer goes silent
// mid-body is failed by the client's idle/progress watchdog (actions'
// downloadIdleTimeout, 120 s) as the transient ErrTransferStalled, and the engine
// then RETRIES it - which is the behaviour this scenario should observe rather than
// time out over. At 180 s the retry had 60 s minus a poll cycle to re-stream the
// whole body, so a correctly recovering client still failed the scenario (the
// mis-sizing recorded in docs/INTEGRATION.md while the -race stalls were being
// investigated). The budget is now the watchdog plus a full second attempt plus
// slack, so a timeout here means the client did NOT recover.
const lfTimeout = 300 * time.Second

// TestIntegration_LargeFileResume drives the large-file resume round-trip end-to-end
// against a live DSM. It first writes a file larger than the configured
// UploadResumeThreshold locally and syncs it: the engine must push it via the chunked
// ResumeUpload path (one frame could not carry it) and the server must end up with a
// byte-for-byte matching MD4. It then replaces that node server-side with a different
// large file (uploaded chunked through the harness's own ResumeUpload helper) and
// asserts the engine streams it back down onto disk intact. The local baseline settles
// to StateSynced after each leg. It self-skips without the SYNOSYNC_IT_* env (see
// docs/INTEGRATION.md).
func TestIntegration_LargeFileResume(t *testing.T) {
	sc := setupScenario(t)

	threshold := sc.conf.Limits.UploadResumeThreshold

	// Outbound: a file comfortably larger than the resume threshold (and spanning
	// several upload chunks) must push via the chunked ResumeUpload path and land on
	// the server with a matching MD4.
	upBody := largeBody(int(threshold)+(3<<20), 0x5a)
	upHash := md4.ContentHash(upBody)
	sc.writeLocal(t, "big.bin", upBody)
	waitForSync(t, sc.eng, lfTimeout, func() bool {
		n, ok := sc.queryRel(t, "big.bin")
		return ok && !n.IsDir && n.Hash == upHash
	})
	assertServerHash(t, sc, "big.bin", upBody)
	assertBaselineSynced(t, sc, "big.bin")

	// Inbound: replace the node server-side with a different large file (pushed
	// through the harness's own chunked ResumeUpload) and assert the engine streams
	// it back down onto disk byte-for-byte.
	downBody := largeBody(int(threshold)+(5<<20), 0xa5)
	sc.serverPutLarge(t, "big.bin", downBody)
	waitForSync(t, sc.eng, lfTimeout, func() bool {
		got, ok := sc.readLocal(t, "big.bin")
		return ok && bytes.Equal(got, downBody)
	})
	assertLocalContent(t, sc, "big.bin", downBody)
	assertBaselineSynced(t, sc, "big.bin")
}

// largeBody returns n bytes of deterministic pseudo-random content seeded by seed. It
// is not a constant fill so the MD4 hash meaningfully exercises the streaming hasher
// and the two round-trip bodies (different seeds) are guaranteed to differ, forcing a
// real inbound modify rather than a no-op.
func largeBody(n int, seed byte) []byte {
	b := make([]byte, n)
	// A trivial xorshift-ish byte generator: deterministic, dependency-free, and
	// good enough to make the content non-uniform across the whole file.
	state := uint32(seed) | 0x100
	for i := range b {
		state = state*1664525 + 1013904223
		b[i] = byte(state >> 24)
	}
	return b
}

// assertServerHash fails unless the server node at a scratch-relative path exists, is a
// file, and its server-reported MD4 matches want - confirming the large outbound upload
// landed intact without downloading the whole body twice.
func assertServerHash(t *testing.T, sc *scenario, rel string, want []byte) {
	t.Helper()
	n, ok := sc.queryRel(t, rel)
	if !ok {
		t.Fatalf("server is missing %q", rel)
	}
	if n.IsDir {
		t.Fatalf("server node %q is a directory, want a file", rel)
	}
	if wantHash := md4.ContentHash(want); n.Hash != wantHash {
		t.Fatalf("server hash for %q = %q, want %q", rel, n.Hash, wantHash)
	}
}

// serverPutLarge uploads content as a file at a scratch-relative path on the server via
// the chunked resume protocol (resume_get_token + offset-based resume_upload), creating
// the intervening scratch directories first. It mirrors the engine's resumeUploadFile
// loop closely enough to stage a large server node for the inbound leg, overwriting any
// existing node at the path. The returned node is the completing chunk's metadata (zero
// if the server reported the transfer already complete).
func (sc *scenario) serverPutLarge(t *testing.T, rel string, content []byte) actions.Node {
	t.Helper()
	dir, name := splitRel(rel)
	parentID := sc.remoteMkdirAll(t, dir).FileID

	ctx, cancel := actionCtx()
	defer cancel()

	size := int64(len(content))
	hash := md4.ContentHash(content)
	tok, err := sc.client.ResumeGetToken(ctx, actions.ResumeRequest{
		ViewID:     sc.view.ViewID,
		RootNodeID: sc.view.RootNodeID,
	})
	if err != nil {
		t.Fatalf("integration: resume_get_token for %q: %v", rel, err)
	}

	// One whole-file two-frame turn: the protocol has no sproto-level chunking and
	// resume_upload carries no offset (see DESIGN §2).
	node, uerr := sc.client.ResumeUpload(ctx, tok.Token, actions.ResumeUploadSpec{
		ParentID:   parentID,
		Name:       name,
		MTime:      time.Now().Unix(),
		RootNodeID: sc.view.RootNodeID,
		Size:       size,
		Hash:       hash,
	}, bytes.NewReader(content))
	if uerr != nil {
		t.Fatalf("integration: resume_upload %q (%d bytes): %v", rel, size, uerr)
	}
	if node.FileID == "" {
		t.Fatalf("integration: resume_upload %q returned no file id", rel)
	}
	return node
}
