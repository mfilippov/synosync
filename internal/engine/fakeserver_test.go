package engine

// The list/get_max_sync_id half of syncClient for the fakes that model a server with no
// tree of their own.
//
// Cold start reads the view's CURRENT tree through `list` now, not the event log, so a
// fake that cannot list cannot cold-start a view - and a view that never cold-starts is
// never subscribed to, so a test waiting on a poll waits forever. These give every such
// fake the one honest answer available to it: the view is empty, and the reply SAYS so.
//
// Evidenced-empty is the important half. A fake returning (nil, false) would model an
// errno-only reply, which servertree.Walk rightly refuses to read as an empty directory;
// a fake that models no tree at all is not making that claim, it genuinely has none.
// A fake that DOES model a tree declares its own List and that one wins.

import (
	"context"

	"github.com/mfilippov/synosync/internal/actions"
)

func (*blockingWakeClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*brokenDeadlineClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*countingErrClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*dlClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*errClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*fakeClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*loopClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*noViewClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*pagePullClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*poisonClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*resumeDLClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*runWakeClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*selectiveFailClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*stallingDownloadClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*starveClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*trackingClient) List(context.Context, int64, string) ([]actions.Node, bool, error) {
	return nil, true, nil
}

func (*blockingWakeClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*brokenDeadlineClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*countingErrClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*deadlineRecordingClient) MaxSyncID(context.Context, int64, int64) (int64, error) {
	return 0, nil
}

func (*dlClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*errClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*fakeClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*loopClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*noViewClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*pagePullClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*poisonClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*pushClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*resumeDLClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*runWakeClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*selectiveFailClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*silentServerClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*stallingDownloadClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*starveClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }

func (*trackingClient) MaxSyncID(context.Context, int64, int64) (int64, error) { return 0, nil }
