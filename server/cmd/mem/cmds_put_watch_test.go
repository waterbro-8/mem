package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PeterGuy326/mem/server/internal/ingest"
	"github.com/spf13/cobra"
)

// The watch fixtures are HTTP-contract level: every request the watcher makes is
// recorded, and a handler that sees anything other than a file upload fails the
// test. That is how AC-003's "no delete propagation, no write-back" is evidenced
// rather than asserted in prose.

type watchUpload struct {
	name   string
	folder string
	body   string
}

type watchStub struct {
	t    *testing.T
	srv  *httptest.Server
	mu   sync.Mutex
	seen []string
	ups  []watchUpload

	// statusFor and dedupeFor let a test decide the fate of one named file.
	statusFor func(name string) int
	dedupeFor func(name string) bool
	// folderFor overrides the returned virtual path so locator honesty can be
	// checked against a server that says something other than what was asked.
	folderFor func(name, requested string) string

	idSeq int
}

func newWatchStub(t *testing.T) *watchStub {
	t.Helper()
	st := &watchStub{t: t}
	st.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		st.seen = append(st.seen, r.Method+" "+r.URL.Path)
		st.mu.Unlock()

		if r.Method != http.MethodPost || r.URL.Path != "/v1/files" {
			st.t.Errorf("watch issued %s %s: only POST /v1/files is allowed", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusTeapot)
			return
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			st.t.Errorf("parse multipart: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		part, hdr, err := r.FormFile("file")
		if err != nil {
			st.t.Errorf("multipart has no file part: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer part.Close()
		b, _ := io.ReadAll(part)
		up := watchUpload{name: hdr.Filename, folder: r.FormValue("path"), body: string(b)}
		if st.statusFor != nil {
			if code := st.statusFor(up.name); code != 0 {
				st.mu.Lock()
				st.ups = append(st.ups, up)
				st.mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]string{"kind": "server", "message": "stub refusal for " + up.name},
				})
				return
			}
		}
		deduped := false
		if st.dedupeFor != nil {
			deduped = st.dedupeFor(up.name)
		}
		virtual := up.folder + "/" + up.name
		if st.folderFor != nil {
			virtual = st.folderFor(up.name, up.folder)
		}
		st.mu.Lock()
		st.idSeq++
		id := fmt.Sprintf("fid-%03d", st.idSeq)
		st.ups = append(st.ups, up)
		st.mu.Unlock()
		status := http.StatusCreated
		if deduped {
			status = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"file":    map[string]any{"id": id, "path": virtual, "name": up.name},
			"deduped": deduped,
		})
	}))
	t.Cleanup(st.srv.Close)
	return st
}

func (s *watchStub) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

func (s *watchStub) uploads() []watchUpload {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]watchUpload(nil), s.ups...)
}

func (s *watchStub) uploaded(name string) int {
	n := 0
	for _, u := range s.uploads() {
		if u.name == name {
			n++
		}
	}
	return n
}

// watchFixture is a watcher wired to the stub over the same config resolution the
// real command uses, so no test exercises a private shortcut.
type watchFixture struct {
	w      *watchRun
	out    *syncBuffer
	errOut *syncBuffer
	root   string
	stub   *watchStub
}

func newWatchFixture(t *testing.T, st *watchStub, to string) *watchFixture {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("MEM_STATE_DIR", filepath.Join(dir, "state"))
	t.Setenv("MEM_CONFIG", filepath.Join(dir, "missing-config.yaml"))
	t.Setenv("MEM_SERVER", st.srv.URL)
	t.Setenv("MEM_TOKEN", "watch-test-token")
	t.Setenv("MEM_WORKSPACE", "")

	cfg, err := resolveConfig("")
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	out := &syncBuffer{}
	errOut := &syncBuffer{}
	cmd := &cobra.Command{Use: "test"}
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	root := filepath.Join(dir, "watched")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := newWatchRun(cmd, newHTTPClient(cfg), root, watchConfig{
		interval: time.Millisecond,
		to:       to,
		format:   "text",
	})
	if err != nil {
		t.Fatalf("newWatchRun: %v", err)
	}
	return &watchFixture{w: w, out: out, errOut: errOut, root: root, stub: st}
}

// cycle runs one scan deterministically, without the poll loop or its sleeps.
func (f *watchFixture) cycle(t *testing.T, n int) watchCycle {
	t.Helper()
	rep, _, err := f.w.runCycle(context.Background(), n)
	if err != nil {
		t.Fatalf("cycle %d: %v", n, err)
	}
	return rep
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// setQuiet moves mtime without touching bytes or length, the way rsync -t, touch
// and a backup restore do.
func setQuiet(t *testing.T, path string, when time.Time) {
	t.Helper()
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
}

// AC-001: a new file is ingested only after it has been quiet for one interval,
// and the cycle counts are right at each step.
func TestWatchIngestsNewFileAfterOneQuietInterval(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Albums")
	writeFile(t, filepath.Join(f.root, "one.txt"), "first")

	c1 := f.cycle(t, 1)
	if c1.Counts != (watchCounts{Scanned: 1}) {
		t.Errorf("cycle 1 counts = %+v, want scanned 1 and nothing ingested yet", c1.Counts)
	}
	if got := st.requests(); len(got) != 0 {
		t.Errorf("a first sighting must not upload: %v", got)
	}

	c2 := f.cycle(t, 2)
	if c2.Counts.Ingested != 1 || c2.Counts.Scanned != 1 {
		t.Errorf("cycle 2 counts = %+v, want scanned 1 ingested 1", c2.Counts)
	}
	ups := st.uploads()
	if len(ups) != 1 || ups[0].name != "one.txt" || ups[0].folder != "/Albums" || ups[0].body != "first" {
		t.Fatalf("uploads = %+v", ups)
	}

	c3 := f.cycle(t, 3)
	if c3.Counts.Unchanged != 1 || c3.Counts.Ingested != 0 {
		t.Errorf("cycle 3 counts = %+v, want unchanged 1", c3.Counts)
	}
	if n := st.uploaded("one.txt"); n != 1 {
		t.Errorf("one.txt uploaded %d times, want 1", n)
	}
}

// AC-001: subdirectories map onto the virtual tree under --to, and a file added
// later does not disturb the count of a file already in the vault.
func TestWatchMapsSubdirectoriesAndCountsMixedStates(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Albums")
	writeFile(t, filepath.Join(f.root, "2012/IMG.jpg"), "sunset")
	f.cycle(t, 1)
	c := f.cycle(t, 2)
	if c.Counts.Ingested != 1 {
		t.Fatalf("counts = %+v, want one ingested", c.Counts)
	}
	if got := st.uploads()[0].folder; got != "/Albums/2012" {
		t.Errorf("folder = %q, want /Albums/2012", got)
	}

	writeFile(t, filepath.Join(f.root, "2013/IMG.jpg"), "snow")
	c = f.cycle(t, 3) // 2013 first sighting, 2012 unchanged
	if c.Counts != (watchCounts{Scanned: 2, Unchanged: 1}) {
		t.Errorf("cycle 3 counts = %+v, want scanned 2 unchanged 1", c.Counts)
	}
	c = f.cycle(t, 4)
	if c.Counts.Ingested != 1 || c.Counts.Unchanged != 1 {
		t.Errorf("cycle 4 counts = %+v, want ingested 1 unchanged 1", c.Counts)
	}
	if got := st.uploads()[1].folder; got != "/Albums/2013" {
		t.Errorf("folder = %q, want /Albums/2013", got)
	}
}

// D-3 ruling 1: mtime moving without the bytes moving must never re-ingest.
func TestWatchTreatsMTimeOnlyChangeAsUnchanged(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	p := filepath.Join(f.root, "notes.md")
	writeFile(t, p, "stable bytes")
	f.cycle(t, 1)
	f.cycle(t, 2)
	if n := st.uploaded("notes.md"); n != 1 {
		t.Fatalf("uploaded %d times before the touch", n)
	}

	setQuiet(t, p, time.Now().Add(2*time.Hour))
	c := f.cycle(t, 3)
	if c.Counts.Unchanged != 1 || c.Counts.Changed != 0 {
		t.Errorf("counts after a touch = %+v, want unchanged 1 and not changed", c.Counts)
	}
	c = f.cycle(t, 4)
	if c.Counts.Unchanged != 1 {
		t.Errorf("counts on the next cycle = %+v, want unchanged 1 (the record absorbed the new mtime)", c.Counts)
	}
	if n := st.uploaded("notes.md"); n != 1 {
		t.Errorf("uploaded %d times, want 1: a touch is not a content change", n)
	}
}

// AC-009: a same-size in-place edit is detected by hash, reported as changed with
// the previously ingested id, and leaves the vault untouched across two cycles.
func TestWatchReportsChangedWithoutReingesting(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	p := filepath.Join(f.root, "report.txt")
	writeFile(t, p, "AAAA")
	f.cycle(t, 1)
	f.cycle(t, 2)
	before := len(st.uploads())

	writeFile(t, p, "BBBB") // same size: only a content hash can see this
	setQuiet(t, p, time.Now().Add(time.Hour))

	c := f.cycle(t, 3)
	if c.Counts.Changed != 1 || c.Counts.Ingested != 0 {
		t.Fatalf("counts = %+v, want changed 1", c.Counts)
	}
	item := c.Items[0]
	if item.State != "changed" || item.Path != p || item.FileID == "" {
		t.Errorf("changed item = %+v, want the local path and the previously ingested id", item)
	}
	if len(st.uploads()) != before {
		t.Errorf("changed content was uploaded: %+v", st.uploads())
	}

	// Two consecutive cycles must leave the vault as it was.
	c = f.cycle(t, 4)
	if c.Counts.Changed != 1 {
		t.Errorf("second cycle counts = %+v, want changed reported again", c.Counts)
	}
	if len(st.uploads()) != before {
		t.Errorf("vault changed across the second cycle: %+v", st.uploads())
	}
	// The record still describes what is in the vault, not what is on disk.
	var rec watchRecord
	found, err := ingest.LoadState(f.w.stateDir, p, &rec)
	if err != nil || !found {
		t.Fatalf("record load found=%v err=%v", found, err)
	}
	if rec.FileID != item.FileID {
		t.Errorf("record file id = %q, want the ingested id %q", rec.FileID, item.FileID)
	}
}

// AC-003: a local deletion is reported and never propagates. The stub fails the
// test on any method other than POST /v1/files, which is the assertion.
func TestWatchLocalDeleteIssuesNoDeleteCall(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	p := filepath.Join(f.root, "temp.txt")
	writeFile(t, p, "keep me in the vault")
	f.cycle(t, 1)
	f.cycle(t, 2)
	if st.uploaded("temp.txt") != 1 {
		t.Fatal("fixture did not ingest the file")
	}

	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	c := f.cycle(t, 3)
	if c.Counts.LocalGone != 1 {
		t.Fatalf("counts = %+v, want local_gone 1", c.Counts)
	}
	item := c.Items[0]
	if item.State != "local_gone" || item.Path != p || item.FileID == "" {
		t.Errorf("local_gone item = %+v", item)
	}
	for _, req := range st.requests() {
		if !strings.HasPrefix(req, "POST /v1/files") {
			t.Errorf("watch made a non-upload request: %s", req)
		}
	}
	// The vault copy stays linked: the record survives so the file is still the
	// same object if it comes back.
	var rec watchRecord
	if found, err := ingest.LoadState(f.w.stateDir, p, &rec); err != nil || !found {
		t.Errorf("record was dropped with the local file: found=%v err=%v", found, err)
	}
}

// D-2: a deduplicated upload reports where the server says the content is, not
// the folder this run asked for.
func TestWatchDedupedItemCarriesServerLocators(t *testing.T) {
	st := newWatchStub(t)
	st.dedupeFor = func(string) bool { return true }
	st.folderFor = func(name, _ string) string { return "/Somewhere/Else/" + name }
	f := newWatchFixture(t, st, "/Albums")
	writeFile(t, filepath.Join(f.root, "dup.txt"), "already in the vault")
	f.cycle(t, 1)
	c := f.cycle(t, 2)

	if c.Counts.Deduped != 1 || c.Counts.Ingested != 0 {
		t.Fatalf("counts = %+v, want deduped 1 and ingested 0", c.Counts)
	}
	item := c.Items[0]
	if item.State != "deduped" || item.VirtualPath != "/Somewhere/Else/dup.txt" || item.FileID == "" {
		t.Errorf("deduped item = %+v, want the server-returned id and path", item)
	}
	if item.SHA256 == "" {
		t.Errorf("deduped item lost its content locator: %+v", item)
	}
}

// AC-002: one failing file gets a stable code, the cycle keeps going, and nothing
// is dropped silently.
func TestWatchFailingFileKeepsCycleGoing(t *testing.T) {
	st := newWatchStub(t)
	st.statusFor = func(name string) int {
		if name == "secret.dat" {
			return http.StatusForbidden
		}
		return 0
	}
	f := newWatchFixture(t, st, "/Docs")
	writeFile(t, filepath.Join(f.root, "secret.dat"), "denied")
	writeFile(t, filepath.Join(f.root, "ok.txt"), "fine")
	f.cycle(t, 1)
	c := f.cycle(t, 2)

	if c.Counts.Failed != 1 || c.Counts.Ingested != 1 {
		t.Fatalf("counts = %+v, want one failed and one ingested in the same cycle", c.Counts)
	}
	if len(c.Failures) != 1 || c.Failures[string(ingest.CodeAuth)] != 1 {
		t.Errorf("failures = %v, want exactly {auth:1}", c.Failures)
	}
	var failed *watchItem
	for i := range c.Items {
		if c.Items[i].State == "failed" {
			failed = &c.Items[i]
		}
	}
	if failed == nil || failed.Code != string(ingest.CodeAuth) || !strings.Contains(failed.Path, "secret.dat") {
		t.Errorf("no located failure: %+v", c.Items)
	}
	// The failed file is offered again next cycle rather than marked done.
	c = f.cycle(t, 3)
	if c.Counts.Failed != 1 {
		t.Errorf("retry cycle counts = %+v, want the failed file retried", c.Counts)
	}
	if n := st.uploaded("secret.dat"); n != 2 {
		t.Errorf("secret.dat attempts = %d, want 2 (one per cycle)", n)
	}
}

// AC-006: the emitted vocabulary is closed, in both directions.
func TestWatchReportVocabularyIsClosed(t *testing.T) {
	st := newWatchStub(t)
	st.statusFor = func(name string) int {
		if name == "quota.bin" {
			return http.StatusTooManyRequests
		}
		return 0
	}
	f := newWatchFixture(t, st, "/Docs")
	f.w.format = "json"
	writeFile(t, filepath.Join(f.root, "quota.bin"), "over quota")
	f.cycle(t, 1)
	c := f.cycle(t, 2)

	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	f.out.Reset()
	f.w.print(c)
	var decoded struct {
		Counts   map[string]int   `json:"counts"`
		Failures map[string]int   `json:"failures"`
		Items    []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	wantCounts := []string{"changed", "deduped", "failed", "ingested", "local_gone", "scanned", "unchanged"}
	if len(decoded.Counts) != len(wantCounts) {
		t.Errorf("counts keys = %v, want exactly %v", keysOf(decoded.Counts), wantCounts)
	}
	for _, k := range wantCounts {
		if _, ok := decoded.Counts[k]; !ok {
			t.Errorf("counts missing %q", k)
		}
	}
	for code := range decoded.Failures {
		if !contains(watchCodes, code) {
			t.Errorf("failure code %q is outside the closed set %v", code, watchCodes)
		}
	}
	for _, it := range decoded.Items {
		state, _ := it["state"].(string)
		if !contains(watchStates, state) {
			t.Errorf("item state %q is outside the closed set %v", state, watchStates)
		}
		if code, ok := it["code"].(string); ok && code != "" && !contains(watchCodes, code) {
			t.Errorf("item code %q is outside the closed set", code)
		}
	}
	if decoded.Failures[string(ingest.CodePlanQuota)] != 1 {
		t.Errorf("failures = %v, want plan_quota 1", decoded.Failures)
	}
	// JSON mode is one machine-readable line, not an indented block.
	if got := strings.Count(f.out.String(), "\n"); got != 1 {
		t.Errorf("json cycle printed %d lines, want 1\n%s", got, f.out.String())
	}
	if !strings.HasPrefix(f.out.String(), `{"contract":"mem.put-watch"`) {
		t.Errorf("json cycle = %s", f.out.String())
	}
}

// SPEC §7.1: --format json makes stdout the machine stream. The banner is the
// one line run() writes outside print, so this goes through the real loop rather
// than the print-level check above: a supervisor must be able to pipe stdout into
// a JSON-lines consumer with no filtering.
func TestWatchJSONModeKeepsStdoutAPureReportStream(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	f.w.format = "json"
	writeFile(t, filepath.Join(f.root, "a.txt"), "a")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.w.run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for strings.Count(f.out.String(), "\n") < 3 {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("stdout stayed short: %s", f.out.String())
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimRight(f.out.String(), "\n"), "\n")
	for i, line := range lines {
		var c watchCycle
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			t.Fatalf("stdout line %d is not a report object: %v\n%s", i+1, err, line)
		}
		if c.Cycle != i+1 {
			t.Errorf("line %d reports cycle %d", i+1, c.Cycle)
		}
	}
	if got := f.errOut.String(); !strings.Contains(got, "watching ") {
		t.Errorf("stderr = %q, want the banner moved there in json mode", got)
	}
	if strings.Contains(f.out.String(), "watching ") {
		t.Error("banner leaked into the json report stream")
	}
}

// AC-007: every cycle appends one object, and the log stops growing at the cap.
func TestWatchReportLogAppendsAndCaps(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	for i := 1; i <= watchReportLogCap+5; i++ {
		c := f.cycle(t, i)
		if err := f.w.persist(c); err != nil {
			t.Fatalf("persist cycle %d: %v", i, err)
		}
	}
	lines := readLogLines(t, f.w.reportLog)
	if len(lines) != watchReportLogCap {
		t.Fatalf("log holds %d lines, want the cap of %d", len(lines), watchReportLogCap)
	}
	var last watchCycle
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		t.Fatal(err)
	}
	if last.Cycle != watchReportLogCap+5 {
		t.Errorf("newest cycle = %d, want %d: rotation must not drop the tail", last.Cycle, watchReportLogCap+5)
	}
	var first watchCycle
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first.Cycle != 6 {
		t.Errorf("oldest retained cycle = %d, want 6", first.Cycle)
	}
	if fi, err := os.Stat(f.w.reportLog); err != nil {
		t.Fatal(err)
	} else if fi.Mode().Perm() != 0o600 {
		t.Errorf("report log mode = %v, want 0600", fi.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(cliStateRoot(), "watch", "reports")); err != nil {
		t.Errorf("report log is not under the CLI state root: %v", err)
	}
	_ = st
}

// AC-005: a root that does not exist fails at startup with not_found, before any
// request is made.
func TestWatchMissingRootExitsNotFound(t *testing.T) {
	st := newWatchStub(t)
	dir := t.TempDir()
	t.Setenv("MEM_STATE_DIR", filepath.Join(dir, "state"))
	t.Setenv("MEM_CONFIG", filepath.Join(dir, "missing-config.yaml"))
	t.Setenv("MEM_SERVER", st.srv.URL)
	t.Setenv("MEM_TOKEN", "tok")
	cfg, err := resolveConfig("")
	if err != nil {
		t.Fatal(err)
	}
	cmd := &cobra.Command{Use: "test"}
	cmd.SetOut(&strings.Builder{})

	_, err = newWatchRun(cmd, newHTTPClient(cfg), filepath.Join(dir, "nope"), watchConfig{
		interval: time.Second, to: "/", format: "text",
	})
	var ce *cliError
	if !errors.As(err, &ce) {
		t.Fatalf("missing root error = %T %v, want *cliError", err, err)
	}
	if ce.code != 2 {
		t.Errorf("code = %d, want 2 (not_found)", ce.code)
	}
	if got := st.requests(); len(got) != 0 {
		t.Errorf("a rejected startup still talked to the server: %v", got)
	}

	// A file is not a directory to watch.
	writeFile(t, filepath.Join(dir, "plain.txt"), "x")
	if _, err := newWatchRun(cmd, newHTTPClient(cfg), filepath.Join(dir, "plain.txt"), watchConfig{
		interval: time.Second, to: "/", format: "text",
	}); err == nil {
		t.Error("watch accepted a file root")
	}
}

// AC-005: one watcher per root. A held lock fails a second watcher fast, with a
// hint that names the file an operator would have to look at.
func TestWatchSecondWatcherFailsFast(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	writeFile(t, filepath.Join(f.root, "a.txt"), "a")

	release, err := acquireWatchLock(f.w.lockPath)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	err = f.w.run(context.Background())
	ce, ok := asCliError(err)
	if !ok {
		release()
		t.Fatalf("contended run error = %T %v, want *cliError", err, err)
	}
	if ce.code != 1 {
		t.Errorf("code = %d, want 1", ce.code)
	}
	if !strings.Contains(ce.hint, filepath.Base(f.w.lockPath)) && !strings.Contains(ce.hint, f.w.lockPath) {
		t.Errorf("hint %q does not name the lock", ce.hint)
	}
	if got := st.requests(); len(got) != 0 {
		t.Errorf("a watcher that lost the lock still talked to the server: %v", got)
	}
	release()

	// Once released, the same root is watchable again with no operator action.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.w.run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("run after the lock was released: %v", err)
	}
}

// AC-005: cancellation ends the run with no error, which the CLI turns into exit
// 0. A stop request is a watcher's expected terminal state.
func TestWatchCancellationExitsZero(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	writeFile(t, filepath.Join(f.root, "a.txt"), "a")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.w.run(ctx); err != nil {
		t.Errorf("cancelled run returned %v, want nil (exit 0)", err)
	}
	if n := len(readLogLines(t, f.w.reportLog)); n != 1 {
		t.Errorf("report log lines = %d, want the one cycle it managed before stopping", n)
	}
}

// D-1: 10 consecutive cycles in which every attempt failed for a non-retryable
// reason give up with that class's exit code instead of spinning silently.
func TestWatchGivesUpAfterTenNonRetryableCycles(t *testing.T) {
	st := newWatchStub(t)
	st.statusFor = func(string) int { return http.StatusForbidden }
	f := newWatchFixture(t, st, "/Docs")
	writeFile(t, filepath.Join(f.root, "a.txt"), "a")

	err := f.w.run(context.Background())
	ce, ok := asCliError(err)
	if !ok {
		t.Fatalf("give-up error = %T %v, want *cliError", err, err)
	}
	if ce.code != 3 {
		t.Errorf("code = %d, want 3 (auth)", ce.code)
	}
	lines := readLogLines(t, f.w.reportLog)
	// The first cycle only observes the file, so it has no attempt to fail. The
	// bound applies to the cycles after it, which is why the log is one longer
	// than watchGiveUpCycles.
	if len(lines) != watchGiveUpCycles+1 {
		t.Errorf("cycles reported = %d, want the observation pass plus %d failing cycles",
			len(lines), watchGiveUpCycles)
	}
	failing := 0
	for _, line := range lines {
		var c watchCycle
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			t.Fatal(err)
		}
		if c.Counts.Failed > 0 {
			failing++
		}
	}
	if failing != watchGiveUpCycles {
		t.Errorf("cycles with failures = %d, want exactly the give-up bound %d", failing, watchGiveUpCycles)
	}
	if n := st.uploaded("a.txt"); n != watchGiveUpCycles {
		t.Errorf("upload attempts = %d, want one per failing cycle", n)
	}
	var last watchCycle
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		t.Fatal(err)
	}
	if last.Counts.Failed != 1 || last.Failures[string(ingest.CodeAuth)] != 1 {
		t.Errorf("final cycle = %+v", last)
	}
}

// D-1's bound must not fire on a transient class: an upload the server rejected
// (409) is worth retrying forever, and only a stop signal ends the run.
func TestWatchDoesNotGiveUpOnRetryableFailures(t *testing.T) {
	st := newWatchStub(t)
	st.statusFor = func(string) int { return http.StatusConflict }
	f := newWatchFixture(t, st, "/Docs")
	writeFile(t, filepath.Join(f.root, "a.txt"), "a")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.w.run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for len(readLogLines(t, f.w.reportLog)) <= watchGiveUpCycles+2 {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("watcher stopped before exceeding %d cycles", watchGiveUpCycles)
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("run returned %v, want nil: a retryable failure must not trigger the give-up bound", err)
	}
	if n := len(readLogLines(t, f.w.reportLog)); n <= watchGiveUpCycles {
		t.Errorf("cycles = %d, want more than the give-up bound", n)
	}
}

// A corrupt record is reported, not silently trusted or fatal.
func TestWatchCorruptRecordIsReportedAndIngestionContinues(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	p := filepath.Join(f.root, "a.txt")
	writeFile(t, p, "content")
	f.cycle(t, 1)

	if err := os.WriteFile(ingest.StatePath(f.w.stateDir, p), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := f.cycle(t, 2)
	if c.Failures[string(ingest.CodeStateCorrupt)] != 1 {
		t.Errorf("failures = %v, want state_corrupt 1", c.Failures)
	}
	if c.Counts.Ingested != 0 {
		t.Errorf("counts = %+v, want a quiet observation pass over the recovered file", c.Counts)
	}
	// Broken state is treated as no state, so the file re-enters through the same
	// quiet-interval gate as a first sighting. Delayed by one interval, never
	// blocked.
	c = f.cycle(t, 3)
	if c.Counts.Ingested != 1 {
		t.Errorf("counts = %+v, want the file ingested anyway (broken state must not block ingestion)", c.Counts)
	}
	if n := c.Failures[string(ingest.CodeStateCorrupt)]; n != 0 {
		t.Errorf("failures = %v, want the rewritten record to read back cleanly", c.Failures)
	}
}

// A cycle interrupted between files must not report a network fault for a run
// that was asked to stop.
func TestWatchUploadCodeComesFromTheSharedClassifier(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	for _, code := range []struct {
		status int
		want   ingest.Code
	}{
		{http.StatusUnauthorized, ingest.CodeAuth},
		{http.StatusPaymentRequired, ingest.CodePlanQuota},
		{http.StatusGatewayTimeout, ingest.CodeProviderTimeout},
		{http.StatusConflict, ingest.CodeUploadRejected},
		{http.StatusBadRequest, ingest.CodeUploadRejected},
	} {
		name := fmt.Sprintf("f%d.txt", code.status)
		st.statusFor = func(n string) int {
			if n == name {
				return code.status
			}
			return 0
		}
		writeFile(t, filepath.Join(f.root, name), "x")
		f.cycle(t, 100+code.status)
		c := f.cycle(t, 101+code.status)
		var found bool
		for _, it := range c.Items {
			if it.State == "failed" && strings.HasSuffix(it.Path, name) {
				found = true
				if it.Code != string(code.want) {
					t.Errorf("HTTP %d classified as %q, want %q", code.status, it.Code, code.want)
				}
			}
		}
		if !found {
			t.Errorf("HTTP %d produced no located failure: %+v", code.status, c.Items)
		}
	}
}

// AC-008: the watcher's records live in the shared keyed store, so the transcript
// sink and the file sink cannot drift on keying or atomicity.
func TestWatchRecordsUseTheSharedKeyedStateStore(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	p := filepath.Join(f.root, "a.txt")
	writeFile(t, p, "shared core")
	f.cycle(t, 1)
	f.cycle(t, 2)

	if got := ingest.StatePath(f.w.stateDir, p); got != ingest.CursorPath(f.w.stateDir, p) {
		t.Errorf("StatePath and CursorPath disagree: %q vs %q", got, ingest.CursorPath(f.w.stateDir, p))
	}
	if _, err := os.Stat(ingest.StatePath(f.w.stateDir, p)); err != nil {
		t.Fatalf("record not written where the core keys it: %v", err)
	}
	if fi, err := os.Stat(ingest.StatePath(f.w.stateDir, p)); err == nil && fi.Mode().Perm() != 0o600 {
		t.Errorf("record mode = %v, want 0600", fi.Mode().Perm())
	}
	// A watcher's records are scoped by root, so two roots over one file cannot
	// answer "did I ingest this?" with each other's state.
	other, err := filepath.Abs(filepath.Join(f.root, "..", "other"))
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(f.w.stateDir) == watchPathsForRoot(other) {
		t.Error("two roots share one record directory")
	}
}

func watchPathsForRoot(abs string) string {
	d, _, _ := watchPaths(abs)
	return filepath.Dir(d)
}

// The walk must not be able to write outside the state root: nothing in watch
// creates or modifies a file under the watched directory.
func TestWatchNeverWritesToTheWatchedTree(t *testing.T) {
	st := newWatchStub(t)
	f := newWatchFixture(t, st, "/Docs")
	writeFile(t, filepath.Join(f.root, "sub", "a.txt"), "a")
	writeFile(t, filepath.Join(f.root, "b.txt"), "b")

	before := treeSnapshot(t, f.root)
	f.cycle(t, 1)
	f.cycle(t, 2)
	f.cycle(t, 3)
	if !jsonMapsEqual(before, treeSnapshot(t, f.root)) {
		t.Errorf("the watched tree changed:\nbefore %v\nafter  %v", before, treeSnapshot(t, f.root))
	}
}

func treeSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		out[p] = fmt.Sprintf("%d|%s", fi.Size(), string(b))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func jsonMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func readLogLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if s := strings.TrimSpace(sc.Text()); s != "" {
			lines = append(lines, s)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

func asCliError(err error) (*cliError, bool) {
	var ce *cliError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}

func contains(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

func keysOf(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// syncBuffer is the fixture's stdout/stderr sink. The watcher writes from its own
// goroutine while a test reads, so the buffers have to be mutex-guarded rather
// than plain strings.Builder values.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
