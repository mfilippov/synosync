package daemon

// The progress half of engineControl for the daemon test fakes.
//
// A fake reports nothing moved, which is what a daemon whose engine has not transferred
// anything reports too - so a status assertion sees the same shape either way, and the
// fakes stay about the control verbs they were written for.

import "time"

func (*fakeEngine) Progress() (int64, int64, time.Duration) { return 0, 0, 0 }

// LastContact reports the zero time: the fake never talked to a server, so `status`
// omits the contact line entirely - the same shape a daemon still in bring-up reports.
func (*fakeEngine) LastContact() time.Time { return time.Time{} }

// Listing reports no walk: the fake never cold-starts a view.
func (*fakeEngine) Listing() (uint64, int64, int64, time.Duration, bool) { return 0, 0, 0, 0, false }

// listingEngine is a fakeEngine that reports a walk in flight.
type listingEngine struct{ *fakeEngine }

func (listingEngine) Listing() (uint64, int64, int64, time.Duration, bool) {
	return 7, 3, 90, time.Minute, true
}
