package engine

// Test for the probe-capability requirement in verifyServerSubtreeAgrees' ROOT-absence arm.
//
// absenceCorroboratedByNodeProbe answers TRUE for a client that does not implement nodeProber - a
// deliberate degradation for its other callers, where it only falls back to the listing-derived
// verdict they already had. On this arm that default is inverted: it turns one ErrPathError listing
// of the delete target straight back into "the subtree is provably empty" and lets the RECURSIVE
// server remove run with nothing enumerated, which is precisely the door the arm was added to shut
// (28672 conflates "no such path" with a server-side timeout). The capability is therefore checked
// first, and its absence is the ordinary transient that leaves the op queued.

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// bareOutboundClient implements outboundClient and NOTHING else - in particular no QueryNode, so
// it is not a nodeProber. The production actions.Client is one; this models the narrow leased
// client the seam admits.
type bareOutboundClient struct {
	listErr error
	removes []string
}

func (c *bareOutboundClient) Upload(context.Context, string, string, int64, int64, []byte) (actions.Node, error) {
	return actions.Node{}, errors.New("unexpected Upload")
}

func (c *bareOutboundClient) Mkdir(context.Context, string, string, int64, int64) (actions.Node, error) {
	return actions.Node{}, errors.New("unexpected Mkdir")
}

func (c *bareOutboundClient) Rename(context.Context, int64, int64, string, string) error {
	return errors.New("unexpected Rename")
}

func (c *bareOutboundClient) Remove(_ context.Context, _ int64, p string) error {
	c.removes = append(c.removes, p)
	return nil
}

func (c *bareOutboundClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, false, c.listErr
}

func (c *bareOutboundClient) ResumeGetToken(context.Context, actions.ResumeRequest) (actions.ResumeToken, error) {
	return actions.ResumeToken{}, errors.New("unexpected ResumeGetToken")
}

func (c *bareOutboundClient) ResumeUpload(context.Context, string, actions.ResumeUploadSpec, io.Reader) (actions.Node, error) {
	return actions.Node{}, errors.New("unexpected ResumeUpload")
}

// TestOutboundRemoveDirRefusesWhenTheClientCannotProbe is the red test: with a client that cannot
// ask the corroborating question, a single path-error listing of the delete root used to return the
// verification nil - authorising the recursive wipe of a subtree nothing had enumerated. It must
// now be the transient errRemoveDirUnverified, which leaves the op queued and never issues the
// destructive turn.
func TestOutboundRemoveDirRefusesWhenTheClientCannotProbe(t *testing.T) {
	c := &bareOutboundClient{listErr: actions.ErrPathError}
	e, _, _ := outboundEngine(t, nil, "Photos", "root1")

	// Sanity: the fake really is outside the corroborating capability, which is the whole
	// premise of this test.
	if _, ok := any(c).(nodeProber); ok {
		t.Fatal("bareOutboundClient implements nodeProber; the fixture no longer models the shape")
	}

	err := e.verifyServerSubtreeAgrees(context.Background(), c, 1, store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "tree",
		FileID: "fd", BaseIsDir: true,
	})

	if !errors.Is(err, errRemoveDirUnverified) {
		t.Fatalf("verifyServerSubtreeAgrees = %v, want errRemoveDirUnverified: an uncorroborated root absence must never license the recursive delete", err)
	}
	if len(c.removes) != 0 {
		t.Fatalf("Remove called %v during verification", c.removes)
	}
}
