package engine

// fakeDSM - a STATEFUL model of the server, as opposed to the scripted stubs that make up
// most of this package's fakes.
//
// Why a second kind of fake exists. The scripted fakes answer a call with a value a test
// wrote down; they are the right tool for "what does the engine do when the server says X".
// They cannot answer "what is left in BOTH replicas after this sequence", because they hold
// no replica: pushClient records that Upload was called, not that the node's content
// changed. Every never-lose claim is of the second kind, so the shapes that matter most were
// only ever asserted against a fake with no server state to lose (docs/DESIGN §12: "a fake
// must never be kinder than the measured server").
//
// This model holds a tree, an event log and a sync-id cursor per view, so a test can mutate
// it as another client would and then assert what survived. Its behaviour is the MEASURED
// behaviour wherever the integration harness measured one:
//
//   - upload is compare_mtime (docs/INTEGRATION.md, TestIntegration_UploadConflictSemantics):
//     a write is ACCEPTED whenever its mtime is not older than the stored node's, and refused
//     only on a strictly older one. A stale base version is NOT rejected - the fake never
//     answers 24579 for one, because the live DSM does not either. This is the single most
//     important asymmetry: pushClient's uploadConflictLeft models a verdict the real server
//     practically never returns, and a keep-both funnel "covered" by it is untested.
//   - remove of a directory is RECURSIVE (TestIntegration_RemoveDirectorySemantics, finding
//     H7): it takes the whole subtree, including children no local baseline ever saw.
//   - pull_event is a windowed LOG, not a snapshot: a page covers a fixed sync-id window and
//     an empty page proves that window only, so the cursor advances past it.
//   - a move event's endpoints are inverted: FromPath is the source, Path the destination.
//
// Where the harness measured nothing, the model says so at the knob rather than picking the
// convenient answer - see dsmRenamePolicy.
//
// Every mutation is attributed to the session of the client that made it, which is what lets
// two engines share one server: each sees the other's writes as remote events and its own as
// self-echo (see twoengine_test.go).

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
)

// dsmRenamePolicy selects what `rename` does when the destination path is already held by
// another node.
//
// This is UNMEASURED: docs/INTEGRATION.md pins rename's absence code and its cursor rule, but
// no test has ever renamed onto an occupied path against a live DSM. The default is therefore
// the destructive possibility, so a client that ships no destination precondition loses the
// occupant visibly in tests instead of passing against a fake that happened to refuse. Flip it
// to dsmRenameRefuses to model the other answer; do not delete the knob until the live
// measurement exists.
type dsmRenamePolicy int

const (
	// dsmRenameReplaces destroys the occupant and moves the source onto its path.
	dsmRenameReplaces dsmRenamePolicy = iota
	// dsmRenameRefuses leaves both nodes alone and answers 40961 (ErrPathError).
	dsmRenameRefuses
)

// dsmNode is one node in a view's tree. A directory carries no content.
type dsmNode struct {
	fileID  string
	isDir   bool
	content []byte
	mtime   int64
}

func (n *dsmNode) hash() string {
	if n.isDir {
		return ""
	}
	return md4.ContentHash(n.content)
}

func (n *dsmNode) size() int64 {
	if n.isDir {
		return 0
	}
	return int64(len(n.content))
}

// dsmView is one shared view: a path-keyed tree, a file-id index over it, and the append-only
// event log the poll reads. Paths are view-relative with no leading slash; the root is "".
type dsmView struct {
	viewID     int64
	rootNodeID int64
	name       string
	privilege  string
	nodes      map[string]*dsmNode
	byID       map[string]string
	events     []actions.Event
	syncID     int64
}

// fakeDSM is the server shared by every client built over it.
type fakeDSM struct {
	mu     sync.Mutex
	views  map[int64]*dsmView
	nextID int

	// pullWindow is the sync-id span one pull_event page covers, whatever it contains. A
	// window holding no surviving events yields an EMPTY page whose cursor still advances,
	// which is the compaction hole the invariant warns about.
	pullWindow int64

	// renamePolicy selects the unmeasured rename-onto-occupied behaviour.
	renamePolicy dsmRenamePolicy

	// renameErrs is popped one per Rename call before any work happens, so a test can script
	// a transient rejection (e.g. &actions.ServerError{Code: actions.CodeStaleCursor}) ahead
	// of the success the retry must reach. A nil entry means "no error, proceed".
	renameErrs []error

	// calls counts the verbs, so a test can assert a precondition probe actually ran.
	uploads    int
	renames    int
	removes    int
	queryNodes int
	lists      int
}

func newFakeDSM() *fakeDSM {
	return &fakeDSM{
		views:        make(map[int64]*dsmView),
		pullWindow:   1000,
		renamePolicy: dsmRenameReplaces,
	}
}

// addView registers a view with an empty tree and returns it. The root node exists from the
// start (a view root is not creatable through the protocol) and carries the file id an upload
// addresses as its parent.
func (d *fakeDSM) addView(viewID, rootNodeID int64, name string) *dsmView {
	d.mu.Lock()
	defer d.mu.Unlock()
	v := &dsmView{
		viewID:     viewID,
		rootNodeID: rootNodeID,
		name:       name,
		privilege:  "rw",
		nodes:      make(map[string]*dsmNode),
		byID:       make(map[string]string),
	}
	root := &dsmNode{fileID: fmt.Sprintf("root-%d", viewID), isDir: true}
	v.nodes[""] = root
	v.byID[root.fileID] = ""
	d.views[viewID] = v
	return v
}

// rootFileID is the parent id an upload into the view root addresses.
func (d *fakeDSM) rootFileID(viewID int64) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.views[viewID].nodes[""].fileID
}

func (d *fakeDSM) newID() string {
	d.nextID++
	return fmt.Sprintf("n%d", d.nextID)
}

// dsmPath normalises a path as it arrives from the engine seam: view-relative, no leading
// slash, with "/" and "." both naming the root.
func dsmPath(p string) string {
	p = strings.Trim(strings.ReplaceAll(p, "\\", "/"), "/")
	if p == "." {
		return ""
	}
	return p
}

func dsmJoin(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

func dsmParent(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[:i]
	}
	return ""
}

func dsmName(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// isDescendant reports whether child lies strictly beneath parent.
func dsmIsDescendant(parent, child string) bool {
	if parent == "" {
		return child != ""
	}
	return strings.HasPrefix(child, parent+"/")
}

func (v *dsmView) actionsNode(p string, n *dsmNode) actions.Node {
	return actions.Node{
		FileID: n.fileID,
		Name:   dsmName(p),
		Path:   p,
		IsDir:  n.isDir,
		Size:   n.size(),
		Hash:   n.hash(),
		MTime:  n.mtime,
	}
}

// logEvent appends one event attributed to session and advances the view's cursor. Callers
// hold d.mu.
func (v *dsmView) logEvent(session, action, p, fromPath string, n *dsmNode) {
	v.syncID++
	ev := actions.Event{
		SyncID:             v.syncID,
		Action:             action,
		FileID:             n.fileID,
		Path:               p,
		FromPath:           fromPath,
		IsDir:              n.isDir,
		KindReported:       true,
		Size:               n.size(),
		Hash:               n.hash(),
		MTime:              n.mtime,
		CommitterSessionID: session,
	}
	v.events = append(v.events, ev)
}

// --- seeding (pre-existing server state, no events) -------------------------------------
//
// Seeded state is what the view already held before any client connected, so it produces NO
// events: an engine learns it through cold start, not through the log. A test that wants the
// server to CHANGE under a running engine uses the verbs instead, through a client bound to
// some other session.

func (d *fakeDSM) seedDir(viewID int64, p string) actions.Node {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seedDirLocked(viewID, dsmPath(p))
}

func (d *fakeDSM) seedDirLocked(viewID int64, p string) actions.Node {
	v := d.views[viewID]
	if n, ok := v.nodes[p]; ok {
		return v.actionsNode(p, n)
	}
	if parent := dsmParent(p); parent != "" {
		d.seedDirLocked(viewID, parent)
	}
	n := &dsmNode{fileID: d.newID(), isDir: true, mtime: 1000}
	v.nodes[p] = n
	v.byID[n.fileID] = p
	return v.actionsNode(p, n)
}

func (d *fakeDSM) seedFile(viewID int64, p string, content []byte, mtime int64) actions.Node {
	d.mu.Lock()
	defer d.mu.Unlock()
	p = dsmPath(p)
	v := d.views[viewID]
	if parent := dsmParent(p); parent != "" {
		d.seedDirLocked(viewID, parent)
	}
	n := &dsmNode{fileID: d.newID(), content: append([]byte(nil), content...), mtime: mtime}
	v.nodes[p] = n
	v.byID[n.fileID] = p
	return v.actionsNode(p, n)
}

// --- assertions helpers ------------------------------------------------------------------

// content returns the bytes the server holds at p, and whether a file stands there.
func (d *fakeDSM) content(viewID int64, p string) ([]byte, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n, ok := d.views[viewID].nodes[dsmPath(p)]
	if !ok || n.isDir {
		return nil, false
	}
	return append([]byte(nil), n.content...), true
}

// paths lists every path the view holds, root excluded, sorted.
func (d *fakeDSM) paths(viewID int64) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for p := range d.views[viewID].nodes {
		if p != "" {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// fileIDAt returns the id of the node standing at p.
func (d *fakeDSM) fileIDAt(viewID int64, p string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n, ok := d.views[viewID].nodes[dsmPath(p)]; ok {
		return n.fileID
	}
	return ""
}

// --- the verbs ---------------------------------------------------------------------------

func (d *fakeDSM) findByFileID(id string) (*dsmView, string, bool) {
	for _, v := range d.views {
		if p, ok := v.byID[id]; ok {
			return v, p, true
		}
	}
	return nil, "", false
}

func (d *fakeDSM) upload(session, parentID, name string, mtime int64, content []byte, isDir bool) (actions.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.uploads++
	v, parentPath, ok := d.findByFileID(parentID)
	if !ok {
		return actions.Node{}, actions.ErrPathError
	}
	if parent := v.nodes[parentPath]; parent == nil || !parent.isDir {
		return actions.Node{}, &actions.ServerError{Code: actions.CodeParentNotADirectory}
	}
	p := dsmJoin(parentPath, name)
	existing := v.nodes[p]
	if existing != nil {
		if existing.isDir != isDir {
			return actions.Node{}, &actions.ServerError{Code: actions.CodeNameTypeCollision}
		}
		// compare_mtime: the write is refused ONLY when it is strictly older than what the
		// server holds. A stale base version is not consulted at all, which is why the
		// client has to prove divergence itself (guardUploadTarget).
		if !isDir && mtime < existing.mtime {
			return actions.Node{}, actions.ErrConflictVersion
		}
	}
	n := existing
	action := "modify"
	if n == nil {
		n = &dsmNode{fileID: d.newID(), isDir: isDir}
		v.nodes[p] = n
		v.byID[n.fileID] = p
		action = "create"
	}
	if !isDir {
		n.content = append([]byte(nil), content...)
	}
	n.mtime = mtime
	v.logEvent(session, action, p, "", n)
	return v.actionsNode(p, n), nil
}

func (d *fakeDSM) rename(session string, viewID int64, from, to string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.renames++
	if len(d.renameErrs) > 0 {
		err := d.renameErrs[0]
		d.renameErrs = d.renameErrs[1:]
		if err != nil {
			return err
		}
	}
	v := d.views[viewID]
	if v == nil {
		return actions.ErrPathError
	}
	from, to = dsmPath(from), dsmPath(to)
	src, ok := v.nodes[from]
	if !ok {
		return &actions.ServerError{Code: actions.CodeMutateNotApplied}
	}
	if parent := v.nodes[dsmParent(to)]; parent == nil || !parent.isDir {
		return &actions.ServerError{Code: actions.CodeMutateNotApplied}
	}
	if _, occupied := v.nodes[to]; occupied {
		if d.renamePolicy == dsmRenameRefuses {
			return &actions.ServerError{Code: actions.CodeMutateNotApplied}
		}
		d.removeSubtreeLocked(session, v, to, false)
	}
	// Move the node and everything beneath it, rewriting the prefix.
	moves := map[string]string{from: to}
	for p := range v.nodes {
		if dsmIsDescendant(from, p) {
			moves[p] = to + p[len(from):]
		}
	}
	relocated := make(map[string]*dsmNode, len(moves))
	for oldPath, newPath := range moves {
		n := v.nodes[oldPath]
		delete(v.nodes, oldPath)
		relocated[newPath] = n
	}
	for newPath, n := range relocated {
		v.nodes[newPath] = n
		v.byID[n.fileID] = newPath
	}
	// One move line for the node itself, with the inverted endpoints the wire uses.
	v.logEvent(session, "move", to, from, src)
	return nil
}

func (d *fakeDSM) remove(session string, viewID int64, p string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.removes++
	v := d.views[viewID]
	if v == nil {
		return actions.ErrPathError
	}
	p = dsmPath(p)
	if _, ok := v.nodes[p]; !ok || p == "" {
		return &actions.ServerError{Code: actions.CodeMutateNotApplied}
	}
	d.removeSubtreeLocked(session, v, p, true)
	return nil
}

// removeSubtreeLocked deletes p and everything beneath it, deepest first, which is the
// RECURSIVE semantic measured on a live DSM: the verb takes children the caller never named.
// withEvents is false for the occupant a replacing rename destroys, whose disappearance the
// move line already reports.
func (d *fakeDSM) removeSubtreeLocked(session string, v *dsmView, p string, withEvents bool) {
	victims := []string{p}
	for q := range v.nodes {
		if dsmIsDescendant(p, q) {
			victims = append(victims, q)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(victims)))
	for _, q := range victims {
		n := v.nodes[q]
		if n == nil {
			continue
		}
		delete(v.nodes, q)
		delete(v.byID, n.fileID)
		if withEvents {
			v.logEvent(session, "delete", q, "", n)
		}
	}
}

func (d *fakeDSM) queryNode(viewID int64, p string) (actions.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queryNodes++
	v := d.views[viewID]
	if v == nil {
		return actions.Node{}, actions.ErrPathError
	}
	p = dsmPath(p)
	n, ok := v.nodes[p]
	if !ok {
		return actions.Node{}, actions.ErrPathError
	}
	return v.actionsNode(p, n), nil
}

func (d *fakeDSM) list(viewID int64, p string) ([]actions.Node, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lists++
	v := d.views[viewID]
	if v == nil {
		return nil, false, actions.ErrPathError
	}
	p = dsmPath(p)
	parent, ok := v.nodes[p]
	if !ok {
		return nil, false, actions.ErrPathError
	}
	if !parent.isDir {
		return nil, false, &actions.ServerError{Code: actions.CodeParentNotADirectory}
	}
	var out []actions.Node
	for q, n := range v.nodes {
		if q != "" && dsmParent(q) == p && dsmIsDescendant(p, q) {
			out = append(out, v.actionsNode(q, n))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	// listed=true: this reply genuinely enumerated the directory, so an empty slice IS proof
	// of an empty directory. A fake that models no tree must answer false instead.
	return out, true, nil
}

func (d *fakeDSM) download(fileID string, w io.Writer) (actions.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, p, ok := d.findByFileID(fileID)
	if !ok {
		return actions.Node{}, actions.ErrPathError
	}
	n := v.nodes[p]
	if n.isDir {
		return actions.Node{}, &actions.ServerError{Code: actions.CodeNameTypeCollision}
	}
	if _, err := w.Write(n.content); err != nil {
		return actions.Node{}, err
	}
	return v.actionsNode(p, n), nil
}

func (d *fakeDSM) maxSyncID(viewID int64) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if v := d.views[viewID]; v != nil {
		return v.syncID
	}
	return 0
}

// pullEvent serves one WINDOW of the log, not one page of a snapshot: the reply covers sync
// ids (since, since+pullWindow] whatever survives in it, and the returned cursor advances to
// the window's end even when the window is empty.
func (d *fakeDSM) pullEvent(viewID, since int64) ([]actions.Event, int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v := d.views[viewID]
	if v == nil {
		return nil, since, actions.ErrPathError
	}
	end := since + d.pullWindow
	if end > v.syncID {
		end = v.syncID
	}
	var out []actions.Event
	for _, ev := range v.events {
		if ev.SyncID > since && ev.SyncID <= end {
			out = append(out, ev)
		}
	}
	if end < since {
		end = since
	}
	return out, end, nil
}

func (d *fakeDSM) longPoll(subs []actions.Subscription) []int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	var changed []int64
	for _, s := range subs {
		if v := d.views[s.ViewID]; v != nil && v.syncID > s.SyncID {
			changed = append(changed, s.ViewID)
		}
	}
	return changed
}

func (d *fakeDSM) userInfo() []actions.View {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []actions.View
	for _, v := range d.views {
		out = append(out, actions.View{
			ViewID:     v.viewID,
			Name:       v.name,
			Privilege:  v.privilege,
			NodeID:     v.nodes[""].fileID,
			RootNodeID: v.rootNodeID,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ViewID < out[j].ViewID })
	return out
}

// --- the client seam ---------------------------------------------------------------------

// dsmClient is one client's view of the shared server. The session id is what every mutation
// is attributed to, so two clients over one fakeDSM see each other's writes as remote and
// their own as self-echo - the property a two-engine scenario is built on.
type dsmClient struct {
	dsm     *fakeDSM
	session string
}

func (c *dsmClient) QueryUserInfo(context.Context) ([]actions.View, error) {
	return c.dsm.userInfo(), nil
}

func (c *dsmClient) QueryNode(_ context.Context, viewID int64, p string) (actions.Node, error) {
	return c.dsm.queryNode(viewID, p)
}

func (c *dsmClient) LongPoll(_ context.Context, subs []actions.Subscription, _ int) ([]int64, error) {
	return c.dsm.longPoll(subs), nil
}

func (c *dsmClient) PullEvent(_ context.Context, viewID, _, since int64) ([]actions.Event, int64, error) {
	return c.dsm.pullEvent(viewID, since)
}

func (c *dsmClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	return c.dsm.download(fileID, w)
}

func (c *dsmClient) MaxSyncID(_ context.Context, viewID, _ int64) (int64, error) {
	return c.dsm.maxSyncID(viewID), nil
}

func (c *dsmClient) List(_ context.Context, viewID int64, p string) ([]actions.Node, bool, error) {
	return c.dsm.list(viewID, p)
}

func (c *dsmClient) Upload(_ context.Context, parentID, name string, mtime, _ int64, content []byte) (actions.Node, error) {
	return c.dsm.upload(c.session, parentID, name, mtime, content, false)
}

func (c *dsmClient) Mkdir(_ context.Context, parentID, name string, mtime, _ int64) (actions.Node, error) {
	return c.dsm.upload(c.session, parentID, name, mtime, nil, true)
}

func (c *dsmClient) Rename(_ context.Context, viewID, _ int64, fromPath, toPath string) error {
	return c.dsm.rename(c.session, viewID, fromPath, toPath)
}

func (c *dsmClient) Remove(_ context.Context, viewID int64, p string) error {
	return c.dsm.remove(c.session, viewID, p)
}

func (c *dsmClient) ResumeGetToken(_ context.Context, req actions.ResumeRequest) (actions.ResumeToken, error) {
	return actions.ResumeToken{Token: fmt.Sprintf("tok-%d", req.ViewID)}, nil
}

func (c *dsmClient) ResumeUpload(_ context.Context, _ string, spec actions.ResumeUploadSpec, body io.Reader) (actions.Node, error) {
	// resume_upload is a WHOLE-FILE turn (docs/INVARIANTS.md): the body carries the entire
	// file, never a suffix, so the model simply reads it out and applies the ordinary write.
	content, err := io.ReadAll(body)
	if err != nil {
		return actions.Node{}, err
	}
	return c.dsm.upload(c.session, spec.ParentID, spec.Name, spec.MTime, content, false)
}

func (c *dsmClient) ResumeDownload(_ context.Context, _, fileID string, _, _, offset int64, w io.Writer) (actions.Node, error) {
	c.dsm.mu.Lock()
	v, p, ok := c.dsm.findByFileID(fileID)
	if !ok {
		c.dsm.mu.Unlock()
		return actions.Node{}, actions.ErrPathError
	}
	n := v.nodes[p]
	content := append([]byte(nil), n.content...)
	node := v.actionsNode(p, n)
	c.dsm.mu.Unlock()
	if offset > int64(len(content)) {
		return actions.Node{}, actions.ErrPathError
	}
	if _, err := w.Write(content[offset:]); err != nil {
		return actions.Node{}, err
	}
	return node, nil
}

// dsmTransport hands every lease the same client, which is correct here: the model is
// internally locked, and a test that wants two clients builds two engines.
type dsmTransport struct {
	client *dsmClient
}

func (t *dsmTransport) EventClient() (syncClient, error) { return t.client, nil }

func (t *dsmTransport) WithWorker(_ context.Context, fn func(syncClient) error) error {
	return fn(t.client)
}

func (t *dsmTransport) ReplaceEvent(context.Context) error { return nil }
