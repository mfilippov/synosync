package actions

import (
	"errors"
	"net"
	"testing"

	"github.com/mfilippov/synosync/internal/sproto"
)

func TestQueryUserInfo(t *testing.T) {
	var gotAction sproto.Value
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		gotAction, _ = req.Get("action")
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("view_list", sproto.Array{
				sproto.NewMap().
					Set("view_id", sproto.Int(1)).
					Set("name", sproto.String("home")).
					Set("privilege", sproto.String("rw")),
				// Second view omits privilege (absent optional field) and adds an
				// extra unknown field that must be ignored.
				sproto.NewMap().
					Set("view_id", sproto.Int(2)).
					Set("name", sproto.String("team")).
					Set("extra", sproto.String("ignored")),
			})
	})

	views, err := c.QueryUserInfo(testContext(t))
	if err != nil {
		t.Fatalf("QueryUserInfo: %v", err)
	}
	if gotAction != sproto.String("query_user_info") {
		t.Errorf("action = %v, want query_user_info", gotAction)
	}
	want := []View{
		{ViewID: 1, Name: "home", Privilege: "rw"},
		{ViewID: 2, Name: "team"},
	}
	if len(views) != len(want) {
		t.Fatalf("got %d views, want %d", len(views), len(want))
	}
	for i := range want {
		if views[i] != want[i] {
			t.Errorf("view[%d] = %+v, want %+v", i, views[i], want[i])
		}
	}
}

func TestQueryUserInfoMissingList(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	views, err := c.QueryUserInfo(testContext(t))
	if err != nil {
		t.Fatalf("QueryUserInfo: %v", err)
	}
	if views != nil {
		t.Errorf("views = %v, want nil for missing view_list", views)
	}
}

// TestQueryUserInfoRejectsBareEmptyReply pins the evidence
// requirement (the enumeration mirror of List's): a bare {} reply - carrying
// NEITHER an errno NOR a view_list - must not decode as an authoritative empty
// enumeration (the engine marks every stored view missing from it absent, which
// would suppress all sync until restart), and, the whole reply having been read,
// must leave the connection byte-aligned and reusable rather than marked broken.
func TestQueryUserInfoRejectsBareEmptyReply(t *testing.T) {
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		// Turn 1: a well-formed but contentless reply - no errno, no view_list.
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		if err := sproto.NewEncoder(server).Encode(sproto.NewMap()); err != nil {
			return
		}
		// Turn 2: a normal evidenced reply, proving the connection survived turn 1.
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		_ = sproto.NewEncoder(server).Encode(sproto.NewMap().Set("errno", sproto.Int(CodeOK)))
	})
	c := newClient(conn, "sess")

	_, err := c.QueryUserInfo(testContext(t))
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply for a bare {} query_user_info reply", err)
	}
	if c.Broken() {
		t.Fatal("connection marked broken; a fully-read bare {} reply must leave it reusable")
	}
	views, err := c.QueryUserInfo(testContext(t))
	if err != nil {
		t.Fatalf("second turn err = %v, want nil (connection should be reusable)", err)
	}
	if views != nil {
		t.Errorf("second turn views = %v, want nil for an errno-evidenced list-less reply", views)
	}
}

func TestQueryUserInfoBadListType(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("view_list", sproto.String("not an array"))
	})
	_, err := c.QueryUserInfo(testContext(t))
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestQueryUserInfoServerError(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodePathError))
	})
	_, err := c.QueryUserInfo(testContext(t))
	if !errors.Is(err, ErrPathError) {
		t.Fatalf("err = %v, want ErrPathError", err)
	}
}

func TestQueryNode(t *testing.T) {
	var gotPath, gotView sproto.Value
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		gotPath, _ = req.Get("path")
		gotView, _ = req.Get("view_id")
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file", sproto.NewMap().
				Set("file_id", sproto.String("f100")).
				Set("name", sproto.String("notes.txt")).
				Set("path", sproto.String("docs/notes.txt")).
				Set("is_dir", sproto.Int(0)).
				Set("size", sproto.Int(1234)).
				Set("hash", sproto.String("deadbeef")).
				Set("mtime", sproto.Int(1700000000)))
	})
	node, err := c.QueryNode(testContext(t), 42, "docs/notes.txt")
	if err != nil {
		t.Fatalf("QueryNode: %v", err)
	}
	if gotPath != sproto.String("docs/notes.txt") {
		t.Errorf("path = %v, want docs/notes.txt", gotPath)
	}
	// The view_id scopes the (otherwise view-ambiguous) path to one view; it must be sent.
	if gotView != sproto.Int(42) {
		t.Errorf("view_id = %v, want 42", gotView)
	}
	want := Node{
		FileID: "f100",
		Name:   "notes.txt",
		Path:   "docs/notes.txt",
		IsDir:  false,
		Size:   1234,
		Hash:   "deadbeef",
		MTime:  1700000000,
	}
	if node != want {
		t.Errorf("node = %+v, want %+v", node, want)
	}
}

func TestQueryNodeDirViaType(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file", sproto.NewMap().
				Set("file_id", sproto.String("d1")).
				Set("name", sproto.String("docs")).
				Set("type", sproto.String("dir")))
	})
	node, err := c.QueryNode(testContext(t), 1, "docs")
	if err != nil {
		t.Fatalf("QueryNode: %v", err)
	}
	if !node.IsDir {
		t.Errorf("IsDir = false, want true for type=dir")
	}
}

func TestQueryNodeMissingFile(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	_, err := c.QueryNode(testContext(t), 1, "x")
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestQueryNodePathError(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodePathError))
	})
	_, err := c.QueryNode(testContext(t), 1, "missing")
	if !errors.Is(err, ErrPathError) {
		t.Fatalf("err = %v, want ErrPathError", err)
	}
}

func TestList(t *testing.T) {
	var gotPath, gotView sproto.Value
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		gotPath, _ = req.Get("path")
		gotView, _ = req.Get("view_id")
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_list", sproto.Array{
				sproto.NewMap().
					Set("file_id", sproto.String("d1")).
					Set("name", sproto.String("sub")).
					Set("is_dir", sproto.Int(1)),
				sproto.NewMap().
					Set("file_id", sproto.String("f2")).
					Set("name", sproto.String("a.txt")).
					Set("size", sproto.Int(10)),
			})
	})
	nodes, listed, err := c.List(testContext(t), 7, "docs")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !listed {
		t.Error("listed = false, want true for a reply carrying file_list")
	}
	if gotPath != sproto.String("docs") {
		t.Errorf("path = %v, want docs", gotPath)
	}
	if gotView != sproto.Int(7) {
		t.Errorf("view_id = %v, want 7", gotView)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	if !nodes[0].IsDir || nodes[0].Name != "sub" {
		t.Errorf("nodes[0] = %+v, want dir sub", nodes[0])
	}
	if nodes[1].IsDir || nodes[1].Size != 10 || nodes[1].FileID != "f2" {
		t.Errorf("nodes[1] = %+v, want file f2 size 10", nodes[1])
	}
}

// TestListEmpty pins the POSITIVE half of the evidence contract: a present-but-empty
// file_list is a real enumeration of a real empty directory, so listed is true and the
// engine's absence provers may consume the emptiness as proof.
func TestListEmpty(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_list", sproto.Array{})
	})
	nodes, listed, err := c.List(testContext(t), 7, "empty")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !listed {
		t.Error("listed = false, want true: file_list: [] IS an enumeration of an empty directory")
	}
	if len(nodes) != 0 {
		t.Errorf("got %d nodes, want 0", len(nodes))
	}
}

// TestListLiveReplyShape pins the shape a live DSM 7.x / Drive Server 4.0.3
// actually sends, which is NOT the one the other List tests exercise: the children
// ride under "node_list" (not "file_list") and the result verdict arrives as a
// nested error{code} block (not a top-level "errno").
//
// Without this the primary decode path was dead in the unit suite: every fixture
// fed the "file_list" FALLBACK, so a regression that broke the "node_list" lookup -
// the only key the real server uses - would have kept the whole package green while
// making List unable to enumerate anything against a live server. Both the
// populated and the empty case are covered, because the empty one is what every
// absence prover consumes as positive proof.
func TestListLiveReplyShape(t *testing.T) {
	okVerdict := func(m *sproto.Map) *sproto.Map {
		return m.Set("error", sproto.NewMap().Set("code", sproto.Int(CodeOK)))
	}

	t.Run("populated", func(t *testing.T) {
		c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
			return okVerdict(sproto.NewMap()).
				Set("node_list", sproto.Array{
					sproto.NewMap().
						Set("file_id", sproto.String("d1")).
						Set("name", sproto.String("sub")).
						Set("file_type", sproto.String("dir")),
					sproto.NewMap().
						Set("file_id", sproto.String("f2")).
						Set("name", sproto.String("a.txt")).
						Set("file_size", sproto.Int(10)),
				})
		})
		nodes, listed, err := c.List(testContext(t), 7, "docs")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if !listed {
			t.Fatal("listed = false, want true: node_list is the key a live DSM sends")
		}
		if len(nodes) != 2 {
			t.Fatalf("got %d nodes, want 2", len(nodes))
		}
		if !nodes[0].IsDir || nodes[0].Name != "sub" {
			t.Errorf("nodes[0] = %+v, want dir sub", nodes[0])
		}
		if nodes[1].IsDir || nodes[1].Size != 10 || nodes[1].FileID != "f2" {
			t.Errorf("nodes[1] = %+v, want file f2 size 10", nodes[1])
		}
	})

	t.Run("empty", func(t *testing.T) {
		c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
			return okVerdict(sproto.NewMap()).Set("node_list", sproto.Array{})
		})
		nodes, listed, err := c.List(testContext(t), 7, "empty")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if !listed {
			t.Error("listed = false, want true: node_list: [] IS an enumeration of an empty directory")
		}
		if len(nodes) != 0 {
			t.Errorf("got %d nodes, want 0", len(nodes))
		}
	})
}

// TestListMissingArray pins the NEGATIVE half: an errno-only reply enumerated nothing,
// so listed is false and the zero-length slice carries no evidence. It is not an error
// (the turn succeeded and the connection stays reusable) - the distinction lives in the
// flag, which every absence prover requires.
func TestListMissingArray(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	nodes, listed, err := c.List(testContext(t), 7, "empty")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if listed {
		t.Error("listed = true, want false for an errno-only reply with no file_list")
	}
	if nodes != nil {
		t.Errorf("nodes = %v, want nil for missing file_list", nodes)
	}
}

// TestListRejectsBareEmptyReply pins the evidence requirement: a
// bare {} reply - carrying NEITHER an errno NOR a file_list - must not decode as an
// authoritative empty listing (the engine's absence probes consume an empty listing
// as positive proof a node is gone), and, the whole reply having been read, must
// leave the connection byte-aligned and reusable rather than marked broken.
func TestListRejectsBareEmptyReply(t *testing.T) {
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		// Turn 1: a well-formed but contentless reply - no errno, no file_list.
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		if err := sproto.NewEncoder(server).Encode(sproto.NewMap()); err != nil {
			return
		}
		// Turn 2: a normal evidenced reply, proving the connection survived turn 1.
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		_ = sproto.NewEncoder(server).Encode(sproto.NewMap().Set("errno", sproto.Int(CodeOK)))
	})
	c := newClient(conn, "sess")

	_, _, err := c.List(testContext(t), 7, "docs")
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply for a bare {} list reply", err)
	}
	if c.Broken() {
		t.Fatal("connection marked broken; a fully-read bare {} reply must leave it reusable")
	}
	nodes, listed, err := c.List(testContext(t), 7, "docs")
	if err != nil {
		t.Fatalf("second turn err = %v, want nil (connection should be reusable)", err)
	}
	if listed {
		t.Error("second turn listed = true, want false for an errno-only reply")
	}
	if nodes != nil {
		t.Errorf("second turn nodes = %v, want nil for an errno-only reply", nodes)
	}
}

func TestListBadEntryType(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_list", sproto.Array{sproto.String("not a map")})
	})
	_, _, err := c.List(testContext(t), 7, "docs")
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}
