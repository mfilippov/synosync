package daemon

import (
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/ipc"
)

// TestListingProgressNamesTheView pins the status plumbing for an initial-sync walk: the
// engine's counters reach the report, the view is named rather than numbered, and an engine
// with no walk running reports none.
func TestListingProgressNamesTheView(t *testing.T) {
	views := []ipc.ViewStatus{{ID: 3, Name: "Team"}, {ID: 7, Name: "home"}}
	got := listingProgress(listingEngine{&fakeEngine{}}, views)
	want := &ipc.ListingProgress{View: "home", Dirs: 3, Entries: 90, For: time.Minute}
	if got == nil || *got != *want {
		t.Fatalf("listingProgress = %+v, want %+v", got, want)
	}
	if got := listingProgress(&fakeEngine{}, views); got != nil {
		t.Errorf("listingProgress with no walk = %+v, want nil", got)
	}
}
