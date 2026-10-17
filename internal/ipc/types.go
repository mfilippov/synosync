// Package ipc is the local-only control-plane transport between the synosync
// daemon and the CLI: a minimal newline-delimited JSON-RPC framing over a
// per-user local transport (a Unix domain socket with perms 0600 on Unix, a
// named pipe with a current-user DACL on Windows), a request-dispatching Server,
// a one-shot Client, and the shared StatusReport type both sides depend on
// without an import cycle.
//
// The transport is intentionally local-only: there is no authentication beyond
// the OS-enforced access control on the transport (the 0600 socket on Unix, the
// current-user-only DACL on the named pipe on Windows), so the listener must
// live in a directory only the owning user can reach (the per-user state dir).
package ipc

import (
	"time"

	"github.com/mfilippov/synosync/internal/config"
)

// StatusReport is the read-only snapshot the daemon returns for the `status`
// method: the paired server address, the per-view sync state, the durable
// pending-op count, and the connection-pool state. It is the shared contract
// between the daemon (producer) and the `status` CLI (consumer).
type StatusReport struct {
	// Server is the paired server address ("host:port").
	Server string `json:"server"`
	// Views lists the per-view sync state, ordered by ascending view id.
	Views []ViewStatus `json:"views"`
	// PendingOps is the number of operations still queued for apply.
	PendingOps int `json:"pending_ops"`
	// DeadLettered is the number of durable dead-letter RECORDS. Most are ops the engine
	// dropped rather than applied (an unappliable server path, a pre-existing symlink,
	// a local non-directory blocking a path, a rename whose feeder was dropped, or a push
	// the server refused the same way for a whole confirmation window). One class is not a
	// dropped OP at all: a fold-key collision between two on-disk names, which never had a
	// queued op, since the path-keyed queue cannot represent two names sharing one key. An
	// outgoing directory delete REFUSED because the server subtree holds content the
	// baseline cannot account for IS a dropped op - the safety refusal is what discards it,
	// and the divergence is also counted under the view's Unresolved. All of them are work
	// the client will not do, so surfacing the count keeps the gap visible.
	DeadLettered int `json:"dead_lettered"`
	// DiskFull is true when the most recent apply failed with ENOSPC (the local disk
	// is out of space). The op is not dropped - it stays queued and retries once space
	// is freed - so this is an operability hint, distinct from DeadLettered.
	DiskFull bool `json:"disk_full"`
	// Stuck is the number of queued ops that have failed the engine's stuck threshold
	// of consecutive applies without being classified terminal (e.g. a permission-denied
	// target, an unresolvable parent). Each is not dropped - it keeps retrying every
	// drain, and any op whose path overlaps queues behind it - so like DiskFull this is
	// an operability hint, distinct from DeadLettered. The counter is in-memory: a
	// daemon restart resets it until the op fails enough drains again. The field is
	// additive in the JSON contract (the daemon and the CLI ship in one binary, an older
	// reader ignores the extra key), so no contract-version bump is needed.
	Stuck int `json:"stuck"`
	// StuckOps names the ops behind that count, worst first and capped, with the reason each
	// keeps failing where the engine has a name for it. The count alone said that something was
	// wrong and nothing about what; the paths and reasons were in the log only. Additive to the
	// JSON contract like Stuck itself.
	StuckOps []StuckEntry `json:"stuck_ops,omitempty"`
	// AppliedOps, TransferredBytes and MovingFor report what the RUNNING daemon has
	// actually moved: operations applied cleanly, bytes of content transferred in either
	// direction, and how long it has been moving them (the window starts at the first
	// applied op, so connecting and planning do not dilute a rate derived from it).
	//
	// They answer the question the rest of this report cannot: whether a sync is slow or
	// stuck. Pending alone does not - a directory level still being applied holds it
	// steady - and until these existed the only way to tell was to watch the sync root
	// with du from outside.
	//
	// The counters are process-local and monotonic: they measure this run, so a restart
	// starts them over. Additive in the JSON contract, so an older reader ignores them.
	AppliedOps int64 `json:"applied_ops"`
	// TransferredBytes counts downloads and uploads only; a mkdir, rename or remove moves
	// no content.
	TransferredBytes int64 `json:"transferred_bytes"`
	// MovingFor is the window the counters accumulated over, zero before the first op.
	MovingFor time.Duration `json:"moving_for"`
	// Recent is the work per second over about the last minute, nil until the daemon has enough
	// history to say; the lifetime average above misleads once the mix of files changes.
	Recent *RecentRate `json:"recent,omitempty"`
	// LastApplied is when the daemon last applied an op, zero before the first.
	LastApplied time.Time `json:"last_applied,omitzero"`
	// ExcludedNames is how many name rules (`synosync exclude --name`) are in force.
	ExcludedNames int `json:"excluded_names,omitempty"`
	// LastContact is when the server last answered this daemon - an action turn that
	// completed or a long poll that returned - or the zero time if it has not since the
	// daemon started. Connection state answers a different question: it describes a
	// socket, and a half-open one, or a server that accepts and never replies, reads as
	// "connected" indefinitely. On an idle sync, which is where a sync client spends most
	// of its life, this is the only evidence that anything is working. Process-local like
	// the counters above. Additive in the JSON contract.
	LastContact time.Time `json:"last_contact"`
	// Listing is the initial-sync walk of the server tree while one runs, nil otherwise. A
	// first sync spends its opening minutes listing - nothing applied, nothing queued - and
	// without this a large view reads as stuck. Additive in the JSON contract.
	Listing *ListingProgress `json:"listing,omitempty"`
	// Connection reports the connection-pool state.
	Connection ConnState `json:"connection"`
	// Paused is true when the user has paused syncing via the `pause` verb. It is
	// distinct from ConnState.Paused, which means the pool paused after a vanished
	// session (8193).
	Paused bool `json:"paused"`
}

// ListingProgress is an initial-sync walk of one view's server tree in flight.
type ListingProgress struct {
	// View names the view being walked.
	View string `json:"view"`
	// Dirs and Entries count the directories listed so far and the entries they held.
	Dirs    int64 `json:"dirs"`
	Entries int64 `json:"entries"`
	// For is how long the walk has been running.
	For time.Duration `json:"for"`
}

// ViewStatus is the per-view slice of a StatusReport.
type ViewStatus struct {
	// ID is the server view id.
	ID uint64 `json:"id"`
	// Name is the human-readable view name.
	Name string `json:"name"`
	// LocalPath is the folder on this computer the view syncs into.
	LocalPath string `json:"local_path,omitempty"`
	// LastSyncID is the highest server sync id consumed for the view.
	LastSyncID int64 `json:"last_sync_id"`
	// InitialSyncDone reports whether the cold-start full-catalog pull completed.
	InitialSyncDone bool `json:"initial_sync_done"`
	// Excluded lists the view's excluded paths (`synosync exclude`), and Rescanning the subtrees
	// still waiting to be walked back in after an exclusion was lifted or a server move brought
	// them in. Both additive.
	Excluded   []string `json:"excluded,omitempty"`
	Rescanning []string `json:"rescanning,omitempty"`
	// Conflicts is the number of outstanding keep-both conflict records (divergent
	// files preserved as _Conflict copies - the durable store conflicts list).
	Conflicts int `json:"conflicts"`
	// Unresolved is the number of baselines stuck in StateConflict: divergences the
	// engine recorded but could not keep-both-resolve (an outbound rename rejected
	// with a stale base, a push to a since-downgraded read-only view, a child under
	// a conflicted parent, or a dead-lettered resolve-conflict op). Unlike Conflicts
	// these have no preserved copy, so they are surfaced as a count rather than in
	// the conflicts list; the count self-clears once a baseline returns to synced.
	Unresolved int `json:"unresolved"`
	// CountsStale is true when Conflicts and Unresolved could not be recomputed for this
	// call - the per-view count scan failed or outran its budget - so they carry the last
	// known values (or zero, if the view has never been counted). The rest of the report is
	// unaffected: the counts are the only O(tree) part of it, and they degrade rather than
	// fail the verb so the scan-free fields (Paused, DiskFull, Stuck, PendingOps) stay
	// readable on exactly the large stores where a count scan cannot fit. The field is
	// additive in the JSON contract (the daemon and the CLI ship in one binary, an older
	// reader ignores the extra key), so no contract-version bump is needed.
	CountsStale bool `json:"counts_stale,omitempty"`
}

// ConflictReport is the read-only snapshot the daemon returns for the
// `conflicts` method: the outstanding keep-both conflict records across all
// views. It is the shared contract between the daemon (producer) and the
// `conflicts` CLI (consumer).
type ConflictReport struct {
	// Conflicts lists the outstanding keep-both conflict records. The daemon caps
	// the list (newest first by DetectedAt) so the encoded report always fits the
	// client's 1 MiB reply cap (maxResponseBytes) - an unbounded list would make
	// every `conflicts` call fail on a truncated read once enough conflicts
	// accumulate.
	Conflicts []ConflictEntry `json:"conflicts"`
	// Total is the full number of outstanding conflict records in the store,
	// which exceeds len(Conflicts) when the report was truncated. Additive in the
	// JSON contract (the daemon and the CLI ship in one binary, an older reader
	// ignores the extra key), so no contract-version bump is needed.
	Total int `json:"total"`
	// Truncated is true when Conflicts was capped and lists only the newest
	// records; Total still reports the full count. Additive like Total.
	Truncated bool `json:"truncated"`
}

// ConflictEntry is a single keep-both conflict record in a ConflictReport.
type ConflictEntry struct {
	// ViewID is the server view id the conflict belongs to.
	ViewID uint64 `json:"view_id"`
	// ViewName is the human-readable view name.
	ViewName string `json:"view_name"`
	// CanonicalPath is the view-relative path of the file that diverged.
	CanonicalPath string `json:"canonical_path"`
	// CopyPath is the view-relative path of the preserved local copy.
	CopyPath string `json:"copy_path"`
	// DetectedAt is the unix time (seconds) the conflict was recorded.
	DetectedAt int64 `json:"detected_at"`
}

// StuckEntry is one op that keeps failing but has not been given up on.
//
// It is the live counterpart of DropEntry: a drop is durable and final, a stuck op is in memory
// and still retrying. Both answer the same operator question - which file, and why - and the
// two lists are deliberately shaped alike.
type StuckEntry struct {
	// ViewID is the view the op targets.
	ViewID uint64 `json:"view_id"`
	// ViewName is the human-readable view name, empty if the view is no longer known.
	ViewName string `json:"view_name"`
	// Op is the operation's type.
	Op string `json:"op"`
	// Direction is the side it applies to.
	Direction string `json:"direction,omitempty"`
	// Path is the view-relative path, NewPath the destination for a move.
	Path    string `json:"path"`
	NewPath string `json:"new_path,omitempty"`
	// Failures is how many consecutive drains it has failed.
	Failures int `json:"failures"`
	// Reason is what the last failure was, when there is a name for it. Empty for an op with
	// no failure of its own - most often one merely blocked behind a failing predecessor.
	Reason string `json:"reason,omitempty"`
}

// DropReport is what the daemon returns for the `drops` method: the durable trace of work
// the engine gave up on, with the reason it recorded for each.
//
// `status` has always counted these ("Dropped: N record(s)") and pointed at the log, which is
// the one place the reason existed. The store has carried the whole record since schema v6 -
// path, direction, node id and a written-out reason - so the count was the least useful part
// of what was already there, and it left the operator grepping a rotating log to answer "which
// file, and why". The engine goes to considerable lengths to record that reason precisely; this
// is the method that lets it reach a person.
//
// Capped and newest-first like ConflictReport, for the same reason: an unbounded list would
// eventually exceed the client's reply cap and make every call fail on a truncated read.
type DropReport struct {
	// Drops lists the recorded drops, newest first.
	Drops []DropEntry `json:"drops"`
	// Total is the full number of records held, which exceeds len(Drops) when truncated.
	Total int `json:"total"`
	// Truncated reports whether Drops lists only the newest slice of Total.
	Truncated bool `json:"truncated"`
}

// DropEntry is one recorded drop.
type DropEntry struct {
	// ViewID is the server view id the dropped work belonged to.
	ViewID uint64 `json:"view_id"`
	// ViewName is the human-readable view name, empty if the view is no longer known.
	ViewName string `json:"view_name"`
	// Op is the dropped operation's type.
	Op string `json:"op"`
	// Direction is the side the operation would have been applied to.
	Direction string `json:"direction,omitempty"`
	// Path is the view-relative path involved. It is empty for the fold-collision overflow
	// summary, which stands for a set of names rather than one path.
	Path string `json:"path"`
	// NewPath is the destination path for a dropped rename; empty otherwise.
	NewPath string `json:"new_path,omitempty"`
	// Reason is the engine's own explanation of why the work was given up on.
	Reason string `json:"reason"`
	// RecordedAt is the unix time (seconds) the drop was recorded.
	RecordedAt int64 `json:"recorded_at"`
}

// ConfigReport is the read-only snapshot the daemon returns for the `config`
// method: the current non-secret settings blob, read live from the state
// database it holds open. It exists so `synosync config get` keeps working
// while the daemon runs: the daemon holds bbolt's
// exclusive file lock, and a read-only bbolt open takes a shared lock that
// conflicts with it (verified empirically), so the CLI cannot read state.db
// directly and instead falls back to this method. The method is additive to
// the contract (the daemon and the CLI ship in one binary, an older reader
// simply never calls it), so no contract-version bump is needed.
type ConfigReport struct {
	// Config is the daemon's current settings blob.
	Config config.Config `json:"config"`
}

// ControlAck is the trivial acknowledgement the daemon returns for the
// live-control verbs (`pause`, `resume`, `sync`): the call succeeded at the IPC
// layer. The verbs have no payload to report beyond success.
type ControlAck struct {
	// OK is always true on a successful control call.
	OK bool `json:"ok"`
}

// The ConnState.PauseReason tokens. They are part of the IPC contract, so they are
// spelled once here and never re-typed at either end.
const (
	PauseSessionExpired = "session_expired"
	PausePinMismatch    = "pin_mismatch"
	PauseClientRejected = "client_rejected"
)

// ConnState reports the connection-pool state.
type ConnState struct {
	// Connecting is true while the daemon has not yet brought the connection
	// pool up - the whole bring-up window, including the retry backoff when the
	// server is unreachable at boot. Neither "connected" nor "paused" is
	// truthful then, so the CLI renders a distinct "connecting" state. The field
	// is additive in the JSON contract: the daemon (producer) and CLI (consumer)
	// ship in one binary and an older reader simply ignores the extra key, so no
	// contract-version bump is needed.
	Connecting bool `json:"connecting"`
	// Reconnecting is true while the pool is up but a dropped worker or event
	// connection is being re-dialed under backoff (a mid-run outage after a
	// successful bring-up - the engine is parked in the pool's reconnect loop, so
	// "connected" would be false for the whole downtime). Distinct from
	// Connecting, which covers only the pre-Start bring-up window. Additive in
	// the JSON contract like Connecting, so no contract-version bump is needed.
	Reconnecting bool `json:"reconnecting"`
	// Paused is true when the pool has paused after a terminal Connector error.
	Paused bool `json:"paused"`
	// PauseReason names WHICH terminal error paused the pool, because the three
	// have three different operator actions and `status` used to print the wrong
	// one for two of them. It carries a stable token, not a message: the daemon
	// classifies (it holds the sentinels) and the CLI words it (it owns the user's
	// vocabulary), so neither has to parse the other's prose. One of:
	//
	//   PauseSessionExpired  - the session token vanished (8193): re-pair.
	//   PausePinMismatch     - the server's certificate no longer matches the pin:
	//                          verify the key change is expected, then re-pair.
	//   PauseClientRejected  - the server rejected this client_version (20481):
	//                          upgrade synosync; re-pairing changes nothing.
	//
	// Empty when the pool is not paused, or paused for a cause this build does not
	// classify - the CLI then says only that the connection is paused rather than
	// naming a fix that may not be the right one. Additive in the JSON contract
	// like Connecting and Reconnecting.
	PauseReason string `json:"pause_reason,omitempty"`
	// NumWorkers is the worker-connection count: the live pool's count once it is
	// up, the configured target while Connecting.
	NumWorkers int `json:"num_workers"`
}

// RecentRate is a rate measured over about the last minute.
type RecentRate struct {
	OpsPerSec   float64 `json:"ops_per_s"`
	BytesPerSec float64 `json:"bytes_per_s"`
}
