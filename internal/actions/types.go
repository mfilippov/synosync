package actions

// This file defines the typed catalog result structs (View, Node) returned by the
// read/metadata actions, their best-effort decode helpers from a *sproto.Map, and
// the small typed map accessors shared by the catalog operations. Decoding is
// deliberately tolerant: an absent field yields the struct's zero value rather than
// an error, so a server that omits an optional field (or adds an extra one) does
// not break a listing. Structural problems - a reply that is the wrong shape
// entirely - are caught by the action methods, which map them to ErrUnexpectedReply.

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/mfilippov/synosync/internal/sproto"
)

// View is one shared folder (Synology "team folder"/view) the account can sync. A
// view is the root namespace that view-relative paths address.
type View struct {
	// ViewID is the server's numeric identifier for the view, used as the
	// addressing root for view-relative paths.
	ViewID int64
	// Name is the human-readable view name.
	Name string
	// Privilege is the access level the account holds on the view (e.g. "rw"),
	// when the server reports it; empty otherwise.
	Privilege string
	// NodeID is the server's opaque node identifier for the view's root
	// directory, used as the parent id when creating top-level entries
	// (id:<NodeID>/<name> addressing). It is empty when query_user_info does not
	// carry it; the engine then resolves it via QueryNode on the view root path
	// ("/"), confirmed against a live DSM.
	NodeID string
	// RootNodeID is the view root's SMALL INTEGER node id from view_list - a
	// different identifier from the file id in NodeID. The upload verb carries it
	// as root_node_id (a wrong or zero value is rejected with 12291) and the
	// long_poll subscribe entry names the same id.
	RootNodeID int64
}

// Node is a single catalog entry: a file or directory. The fields mirror what the
// server reports for query_node/list and the transfer replies; unreported fields
// stay zero.
type Node struct {
	// FileID is the server's opaque node identifier, used for id:<file_id>
	// addressing by download and as the parent for uploads.
	FileID string
	// Name is the leaf name within its parent.
	Name string
	// Path is the view-relative path the node was addressed by, when reported.
	Path string
	// IsDir reports whether the node is a directory.
	IsDir bool
	// Size is the content length in bytes (0 for directories).
	Size int64
	// Hash is the server-reported MD4 content hash for files, folded to
	// lowercase hex at decode (see canonicalHash).
	Hash string
	// MTime is the modification time as the server's integer timestamp.
	MTime int64
}

// decodeView reads a View from a node map, tolerating absent fields.
func decodeView(m *sproto.Map) View {
	var v View
	if id, ok := m.Int("view_id"); ok {
		v.ViewID = id
	}
	if name, ok := m.String("name"); ok {
		v.Name = name
	}
	if priv, ok := m.String("privilege"); ok {
		v.Privilege = priv
	}
	// NodeID must be the view root's FILE ID - the value upload addresses a parent
	// by (id:<parent_file_id>/<name>). view_list's integer "node_id" is a DIFFERENT
	// identifier (1 for the first view) and must NOT be used here: sending it as an
	// upload parent is rejected with 32769. When view_list carries no file_id the
	// field stays empty and the engine resolves the real one via QueryNode on the
	// view root path.
	if id, ok := m.String("file_id"); ok {
		v.NodeID = id
	} else if id, ok := m.String("node_id"); ok {
		v.NodeID = id
	}
	// The small integer node id rides alongside; upload's root_node_id and the
	// long_poll subscribe entry both name it (see View.RootNodeID).
	if id, ok := m.Int("node_id"); ok {
		v.RootNodeID = id
	}
	return v
}

// decodeNode reads a Node from a node map, tolerating absent fields.
func decodeNode(m *sproto.Map) Node {
	var n Node
	if id, ok := m.String("file_id"); ok {
		n.FileID = id
	}
	if name, ok := m.String("name"); ok {
		n.Name = name
	}
	if p, ok := m.String("path"); ok {
		n.Path = viewRelative(p)
	}
	// The wire names are file_size / file_hash; the bare "size"/"hash" forms are
	// accepted as a fallback for the in-memory fakes.
	if size, ok := m.Int("file_size"); ok {
		n.Size = size
	} else if size, ok := m.Int("size"); ok {
		n.Size = size
	}
	if h, ok := m.String("file_hash"); ok {
		n.Hash = canonicalHash(h)
	} else if h, ok := m.String("hash"); ok {
		n.Hash = canonicalHash(h)
	}
	if mt, ok := m.Int("mtime"); ok {
		n.MTime = mt
	}
	n.IsDir = nodeIsDir(m)
	return n
}

// canonicalHash folds a server-reported content hash to the canonical lowercase
// hex form every locally computed MD4 uses (hex.EncodeToString / md4.ContentHash).
// The server's hex casing is not a confirmed contract, and every downstream
// consumer - the Download/ResumeDownload MD4 verify, the engine's divergence
// gates and false-conflict adoption, the baseline records - compares hashes by
// byte equality; normalizing at this single ingestion point keeps those
// comparisons correct against an uppercase-hex server without scattering
// strings.EqualFold across call sites.
func canonicalHash(h string) string {
	return strings.ToLower(h)
}

// Event is a single change reported by a view's event stream (pull_event). Events
// are the engine's source of truth for what changed remotely since a given sync
// cursor; CommitterSessionID lets the engine drop self-echo (changes this client
// itself committed - see DESIGN §6). Unreported fields stay zero, mirroring the
// tolerant decode used for catalog nodes.
type Event struct {
	// SyncID is the monotonic change cursor this event was recorded at; the
	// engine advances its per-view cursor past the highest SyncID it has applied.
	SyncID int64
	// Action is the kind of change: "create", "modify", "delete" or "move".
	Action string
	// FileID is the server's opaque node identifier the change applies to.
	FileID string
	// Path is the view-relative path of the affected node (the destination path
	// for a move).
	Path string
	// IsDir reports whether the affected node is a directory.
	IsDir bool
	// KindReported records whether the event map actually carried the node's kind
	// ("is_dir" or "type"). IsDir is a plain bool, so an absent key is indistinguishable
	// from a reported "this is not a directory" - and a move event is routinely sparse
	// (see the engine's MovedBaseIsDir capture, which reads the kind from the baseline
	// for exactly that reason). A consumer that must MATERIALISE the node from the event
	// alone, with no baseline to fall back on, needs the difference: see
	// engine.rescueUncontainableSource.
	KindReported bool
	// Size is the content length in bytes after the change (0 for directories or
	// deletes).
	Size int64
	// Hash is the server-reported MD4 content hash after the change, for files,
	// folded to lowercase hex at decode (see canonicalHash).
	Hash string
	// MTime is the modification time as the server's integer timestamp.
	MTime int64
	// CommitterSessionID identifies the session that committed the change, used to
	// recognise and skip this client's own echoes.
	CommitterSessionID string
	// FromPath is the source path of a move, when the event reports one; empty
	// for non-move events.
	FromPath string
	// FromFileID is the source node id of a move, when reported; empty otherwise.
	FromFileID string
}

// decodeEvent reads an Event from an event map, tolerating absent fields in the
// same best-effort spirit as decodeNode.
func decodeEvent(m *sproto.Map) Event {
	var e Event
	if id, ok := m.Int("sync_id"); ok {
		e.SyncID = id
	}
	// The wire name for the change kind is "type" ("create"/"modify"/"remove"/
	// "move"); "action" is the fallback the in-memory fakes speak.
	if a, ok := m.String("type"); ok {
		e.Action = normalizeEventAction(a)
	} else if a, ok := m.String("action"); ok {
		e.Action = normalizeEventAction(a)
	}
	// The top-level file_id is an INTEGER on the wire (the nested file.parent_id is
	// a string - the protocol is inconsistent here). Reading only the string form
	// left every event with an empty id.
	if id, ok := m.String("file_id"); ok {
		e.FileID = id
	} else if id, ok := m.Int("file_id"); ok {
		e.FileID = strconv.FormatInt(id, 10)
	}
	if p, ok := m.String("path"); ok {
		e.Path = viewRelative(p)
	}
	// The node's attributes ride in a nested "file" sub-map under their file_*
	// names; the flat forms are the fakes' fallback.
	attrs := m
	if raw, ok := m.Get("file"); ok {
		if fm, ok := raw.(*sproto.Map); ok {
			attrs = fm
		}
	}
	if size, ok := attrs.Int("file_size"); ok {
		e.Size = size
	} else if size, ok := m.Int("size"); ok {
		e.Size = size
	}
	if h, ok := attrs.String("file_hash"); ok {
		e.Hash = canonicalHash(h)
	} else if h, ok := m.String("hash"); ok {
		e.Hash = canonicalHash(h)
	}
	if mt, ok := attrs.Int("mtime"); ok {
		e.MTime = mt
	} else if mt, ok := m.Int("mtime"); ok {
		e.MTime = mt
	}
	if cs, ok := m.String("committer_session_id"); ok {
		e.CommitterSessionID = cs
	}
	if fp, ok := m.String("from_path"); ok {
		e.FromPath = viewRelative(fp)
	}
	// A move's two endpoints are INVERTED relative to what this decoder assumed:
	// on the wire the top-level "path" is the SOURCE (it mirrors source.path) and the
	// new location rides in destination.path. Reading the top level as the
	// destination left every inbound move with an empty FromPath, which reconcile
	// then skipped as non-containable - so a server-side rename never reached disk.
	if dst, ok := m.Get("destination"); ok {
		if dm, ok := dst.(*sproto.Map); ok {
			if p, ok := dm.String("path"); ok && p != "" {
				if e.FromPath == "" {
					e.FromPath = e.Path
				}
				e.Path = viewRelative(p)
			}
		}
	}
	if e.FromPath == "" {
		if src, ok := m.Get("source"); ok {
			if sm, ok := src.(*sproto.Map); ok {
				if p, ok := sm.String("path"); ok {
					e.FromPath = viewRelative(p)
				}
			}
		}
	}
	if fid, ok := m.String("from_file_id"); ok {
		e.FromFileID = fid
	}
	// The kind lives with the other attributes (file.file_type); fall back to the
	// event map itself for the fakes.
	if e.IsDir, e.KindReported = nodeIsDirEvidenced(attrs); !e.KindReported {
		e.IsDir, e.KindReported = nodeIsDirEvidenced(m)
	}
	return e
}

// decodeEvents decodes an event_list array into a slice of Events. Each element
// must be a Map; a wrong-typed entry yields ErrUnexpectedReply. An empty array
// yields an empty (non-nil) slice.
func decodeEvents(arr sproto.Array) ([]Event, error) {
	events := make([]Event, 0, len(arr))
	for _, el := range arr {
		m, ok := el.(*sproto.Map)
		if !ok {
			return nil, fmt.Errorf("actions: event_list entry is %T, want map: %w", el, ErrUnexpectedReply)
		}
		events = append(events, decodeEvent(m))
	}
	return events, nil
}

// decodeChanges decodes a changes array (a long_poll result) into the view ids
// that have pending changes. Each element must be an Int; a wrong-typed entry
// yields ErrUnexpectedReply. An empty array yields an empty (non-nil) slice.
func decodeChanges(arr sproto.Array) ([]int64, error) {
	ids := make([]int64, 0, len(arr))
	for _, el := range arr {
		iv, ok := el.(sproto.Int)
		if !ok {
			return nil, fmt.Errorf("actions: changes entry is %T, want int: %w", el, ErrUnexpectedReply)
		}
		ids = append(ids, int64(iv))
	}
	return ids, nil
}

// ResumeToken is the server's handle for an in-progress large-file transfer,
// returned by ResumeGetToken. It is threaded through the subsequent
// ResumeUpload/ResumeDownload turns. The token has no durable cross-process
// meaning here; persisting it across runs is left to the sync engine (Phase 6).
type ResumeToken struct {
	// Token is the opaque server-issued resume handle.
	Token string
	// Offset is the byte offset the server has already committed for this
	// transfer at the time the token was issued (0 for a fresh transfer).
	Offset int64
	// Size is the total expected content length in bytes, when the server
	// reports it; zero otherwise.
	Size int64
}

// decodeResumeToken reads a ResumeToken from a reply map, tolerating absent
// fields and accepting either the "resume_token" or "token" key for the handle.
func decodeResumeToken(m *sproto.Map) ResumeToken {
	var t ResumeToken
	if tok, ok := m.String("resume_token"); ok {
		t.Token = tok
	} else if tok, ok := m.String("token"); ok {
		t.Token = tok
	}
	if off, ok := m.Int("offset"); ok {
		t.Offset = off
	}
	if size, ok := m.Int("size"); ok {
		t.Size = size
	}
	return t
}

// nodeIsDir reports whether a node map describes a directory. The server may carry
// this either as an "is_dir" integer flag or as a "type" string; both are honoured.
// A map carrying neither reads as "not a directory", the tolerant-decode default every
// caller that has another source for the kind relies on.
func nodeIsDir(m *sproto.Map) bool {
	isDir, _ := nodeIsDirEvidenced(m)
	return isDir
}

// nodeIsDirEvidenced is nodeIsDir plus the evidence: reported is false when the map
// carried NEITHER key, so a caller that would otherwise read the tolerant default as a
// positive "this is a file" can tell the two apart (Event.KindReported).
func nodeIsDirEvidenced(m *sproto.Map) (isDir, reported bool) {
	// file_type is the wire name ("file" | "dir" | "link"); is_dir/type are the
	// fallbacks the in-memory fakes speak.
	if t, ok := m.String("file_type"); ok {
		return t == "dir", true
	}
	if d, ok := m.Int("is_dir"); ok {
		return d != 0, true
	}
	if t, ok := m.String("type"); ok {
		return t == "dir", true
	}
	return false, false
}

// normalizeEventAction maps the wire's change-kind vocabulary onto the one the
// engine switches on. The server says "remove" where the engine says "delete";
// every other kind ("create", "modify", "move") is already identical. Translating
// at this single decode point keeps the wire vocabulary out of the engine - an
// unmapped "remove" fell through every switch, so a server-side deletion was
// silently ignored.
func normalizeEventAction(a string) string {
	if a == "remove" {
		return "delete"
	}
	return a
}

// viewRelative converts a server path to the view-RELATIVE form the engine works
// in. The wire reports paths rooted at the view ("/", "/notes.txt",
// "/subdir/nested.txt"); the engine's containment gate is filepath.IsLocal, which
// rejects every one of those as absolute. Left unstripped, EVERY inbound event was
// silently skipped as non-containable - the client applied nothing from the server
// while reporting a healthy sync. The view root maps to the empty relative path.
func viewRelative(p string) string {
	return strings.TrimPrefix(p, "/")
}
