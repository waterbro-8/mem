package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/PeterGuy326/mem/server/internal/apiclient"
	"github.com/PeterGuy326/mem/server/internal/ingest"
	"github.com/spf13/cobra"
)

// mem put <dir> --watch is the one-way local→mem watcher (SPEC.md:604, issue
// #110). Its shape was adjudicated on the issue, and the four rulings are load
// bearing enough that the code names them:
//
//	D-1 foreground poll loop. No daemonize, no pidfile, no service unit, no
//	    fsnotify. SIGINT/SIGTERM end the run between cycles with exit 0; one
//	    advisory lock per watched root; a root that does not exist at startup exits
//	    2 without entering the loop; 10 consecutive cycles in which every attempt
//	    failed with a non-retryable class give up with that class's code.
//	D-2 one closed-vocabulary report per cycle on stdout, plus a capped append-only
//	    JSONL log under the CLI state root. No server-side ledger.
//	D-3 (size, mtime) is the cost gate, sha256 is the authority, and a file must
//	    hold still for one interval before it is uploaded. A content change to
//	    something already ingested is reported as "changed" and NOT re-ingested:
//	    the file plane has no revision model, so a second upload of one path would
//	    be a second unlinked Drive entry. A local delete is reported as "local_gone"
//	    and issues no delete call.
//	D-4 one shared ingestion core, two sinks. Walk, keyed atomic state, the failure
//	    code vocabulary and classification come from internal/ingest; only the
//	    write API differs (multipart /v1/files here, POST /v1/memories for the
//	    transcript connector).
//
// Boundaries that must not erode (REQ-003): nothing here writes to the local disk
// outside the CLI state root, nothing deletes remote content, and nothing pulls
// content back down. Full sync stays Phase 2 (SPEC.md:1140, GOAL.md:95/123).

const (
	watchContract      = "mem.put-watch"
	watchSchemaVersion = 1
	// watchReportLogCap bounds the JSONL log so a watcher that runs for months
	// cannot fill the user's disk (D-2).
	watchReportLogCap = 200
	// watchGiveUpCycles is D-1's bound: enough to ride out a brief credential
	// flap, small enough that a supervisor restarts a process that reported a
	// real error rather than one that spun silently forever.
	watchGiveUpCycles = 10
	// watchIntervalDefault is D-1's default poll interval.
	watchIntervalDefault = 30 * time.Second
)

// errWatchLocked marks contention for a root. The lock helpers live behind
// platform files; this is the error they share.
var errWatchLocked = errors.New("watch lock held by another process")

// watchStates and watchCodes are the closed sets AC-006 pins. The code set is
// ingest's, not a watch-local alias for the same conditions.
var (
	watchStates = []string{"ingested", "deduped", "changed", "local_gone", "failed"}
	watchCodes  = []string{
		string(ingest.CodeAuth), string(ingest.CodePlanQuota), string(ingest.CodeProviderTimeout),
		string(ingest.CodeNetwork), string(ingest.CodeReadDenied), string(ingest.CodeUploadRejected),
		string(ingest.CodeRootMissing), string(ingest.CodeStateCorrupt),
	}
)

// watchItem is one located outcome. Locators come from the server's response, so
// a deduplicated upload reports where the content actually is rather than the
// folder this run asked for: Service.Put answers identical content with the
// pre-existing row (秒传), whose virtual path can be anywhere in the workspace.
type watchItem struct {
	Path        string `json:"path"`
	State       string `json:"state"`
	FileID      string `json:"file_id,omitempty"`
	VirtualPath string `json:"virtual_path,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	Code        string `json:"code,omitempty"`
	Detail      string `json:"detail,omitempty"`
}

// watchCounts is the closed count set. Every field is always emitted, including
// at zero, so a consumer can tell "nothing changed" from "not measured".
type watchCounts struct {
	Scanned   int `json:"scanned"`
	Ingested  int `json:"ingested"`
	Deduped   int `json:"deduped"`
	Unchanged int `json:"unchanged"`
	Changed   int `json:"changed"`
	LocalGone int `json:"local_gone"`
	Failed    int `json:"failed"`
}

// watchCycle is one cycle's report: the object printed to stdout and the object
// appended to the JSONL log, the same content in both formats.
type watchCycle struct {
	Contract      string         `json:"contract"`
	SchemaVersion int            `json:"schema_version"`
	Cycle         int            `json:"cycle"`
	Root          string         `json:"root"`
	To            string         `json:"to"`
	StartedAt     string         `json:"started_at"`
	DurationMS    int64          `json:"duration_ms"`
	Counts        watchCounts    `json:"counts"`
	Failures      map[string]int `json:"failures,omitempty"`
	Items         []watchItem    `json:"items,omitempty"`
}

// watchRecord is the persisted per-path state. An empty IngestedAt means the
// record is only a stability observation: the file was seen, and the next cycle
// decides whether it held still long enough to be uploaded.
type watchRecord struct {
	Abs         string `json:"abs"`
	Size        int64  `json:"size"`
	ModTime     string `json:"mtime"`
	SHA256      string `json:"sha256,omitempty"`
	FileID      string `json:"file_id,omitempty"`
	VirtualPath string `json:"virtual_path,omitempty"`
	IngestedAt  string `json:"ingested_at,omitempty"`
}

// watchRun is one watcher: its configuration and the paths it owns.
type watchRun struct {
	client     *httpClient
	cmd        *cobra.Command
	root       string
	to         string
	tags       []string
	format     string
	interval   time.Duration
	stateDir   string
	reportLog  string
	lockPath   string
	sourceMeta *apiclient.FileSourceMetadata

	// now is a seam so a test can pin timestamps without pinning the loop.
	now func() time.Time
}

func watchKey(abs string) string {
	sum := sha1.Sum([]byte(abs))
	return hex.EncodeToString(sum[:])
}

// watchPaths returns the state locations for one watched root. Everything lives
// under cliStateRoot(), the same $HOME/.mem the transcript connector uses, so one
// environment variable (MEM_STATE_DIR) relocates all CLI state.
//
// Records are scoped by root: the same file under two watched roots gets two
// records, because the two watchers can be pointed at different folders and one
// cursor could not answer "did *this* watcher ingest it?" honestly.
func watchPaths(absRoot string) (stateDir, reportLog, lockPath string) {
	base := filepath.Join(cliStateRoot(), "watch")
	scoped := filepath.Join(base, watchKey(absRoot))
	return filepath.Join(scoped, "cursors"),
		filepath.Join(base, "reports", watchKey(absRoot)+".jsonl"),
		filepath.Join(base, "locks", watchKey(absRoot)+".lock")
}

// newWatchRun validates the flag combination and resolves every path the loop
// needs. A non-directory root is rejected rather than guessed at: recursion is
// implied by watching, and in this tier the directory the user picks *is* the
// boundary (no ignore language), so a single file has no meaning here that
// `mem put <path>` does not already have.
func newWatchRun(cmd *cobra.Command, c *httpClient, target string, w watchConfig) (*watchRun, error) {
	if w.interval <= 0 {
		return nil, newCliError(1, "--interval must be positive", "")
	}
	abs, err := filepath.Abs(target)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return nil, newCliError(2, "watch root does not exist: "+abs,
			"watch does not start on a missing directory: create it or fix the path")
	}
	if !fi.IsDir() {
		return nil, newCliError(2, "watch root is not a directory: "+abs,
			"pass the directory to watch; a single file belongs in `mem put <path>`")
	}
	stateDir, reportLog, lockPath := watchPaths(abs)
	return &watchRun{
		client:     c,
		cmd:        cmd,
		root:       abs,
		to:         w.to,
		tags:       w.tags,
		format:     w.format,
		interval:   w.interval,
		stateDir:   stateDir,
		reportLog:  reportLog,
		lockPath:   lockPath,
		sourceMeta: w.sourceMeta,
		now:        time.Now,
	}, nil
}

// watchConfig carries what --watch needs from the command surface.
type watchConfig struct {
	interval   time.Duration
	to         string
	tags       []string
	format     string
	sourceMeta *apiclient.FileSourceMetadata
}

// run is D-1's loop: lock, cycle, wait, repeat. It returns nil on cancellation,
// because for a watcher being stopped is the expected terminal state and any
// non-zero code makes every wrapper think it failed.
func (w *watchRun) run(ctx context.Context) error {
	release, err := acquireWatchLock(w.lockPath)
	if err != nil {
		if errors.Is(err, errWatchLocked) {
			return newCliError(1, "another mem watch already holds "+w.root,
				"one watcher per root: look for a running `mem put --watch`, and remove "+w.lockPath+" only if its holder is gone")
		}
		return err
	}
	defer release()

	// SIGINT already arrives on the root context; SIGTERM has to be added here so
	// `docker stop` and a systemd stop end the run between cycles instead of
	// leaving the process to be killed while it streams an upload.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	w.diagf("watching %s -> %s every %s (Ctrl-C or SIGTERM to stop; state %s)\n",
		w.root, w.to, w.interval, filepath.Dir(w.stateDir))

	cycle := 0
	streak := 0
	for {
		cycle++
		report, attempts, err := w.runCycle(ctx, cycle)
		if err != nil {
			return err
		}
		if perr := w.persist(report); perr != nil {
			w.diagf("warn: report log: %v\n", perr)
		}
		w.print(report)

		// The bound counts cycles, not files: one bad credential in a tree of
		// ten thousand files is a reason to stop, and one rejected file among
		// successes is not.
		if attempts == 0 || report.Counts.Failed != attempts || !allFailuresNonRetryable(report) {
			streak = 0
		} else {
			streak++
		}
		if streak >= watchGiveUpCycles {
			return newCliError(giveUpExit(report),
				fmt.Sprintf("watch gave up after %d cycles of non-retryable failures", streak),
				"fix the credential, plan or server the last report names; a supervisor may restart this process")
		}

		if !sleepContext(ctx, w.interval) {
			return nil
		}
	}
}

// sleepContext waits for d, reporting false when ctx was cancelled first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// runCycle scans once and acts on what it observed. It returns the cycle report
// and how many upload attempts it made, because the give-up bound has to
// distinguish "every attempt failed" from "there were no attempts".
func (w *watchRun) runCycle(ctx context.Context, n int) (watchCycle, int, error) {
	started := w.now()
	report := watchCycle{
		Contract:      watchContract,
		SchemaVersion: watchSchemaVersion,
		Cycle:         n,
		Root:          w.root,
		To:            w.to,
		StartedAt:     started.UTC().Format(time.RFC3339Nano),
		Failures:      map[string]int{},
	}
	fail := func(code ingest.Code) { report.Failures[string(code)]++ }

	if _, err := os.Stat(w.root); err != nil {
		// The root can go away after startup (unmounted volume, renamed parent).
		// root_missing keeps reporting rather than exiting: the directory may come
		// back, and the next cycle should pick it up without a restart.
		fail(ingest.CodeRootMissing)
		report.Counts.Failed++
		report.Items = append(report.Items, watchItem{Path: w.root, State: "failed",
			Code: string(ingest.CodeRootMissing), Detail: "watch root is not readable right now"})
		return w.finish(report, started), 0, nil
	}

	paths, err := ingest.Walk(w.root, nil)
	if err != nil {
		return report, 0, err
	}
	attempts := 0

	for _, abs := range paths {
		if ctx.Err() != nil {
			// A signal stops the run at a file boundary, so a large tree cannot
			// hold a process that has already been asked to exit.
			break
		}
		report.Counts.Scanned++

		size, mtime, err := ingest.FileState(abs)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// Vanished between the walk and the stat. That is not a failed
				// ingestion attempt, so it is not counted as one; the next cycle
				// reports whatever is actually there.
				report.Counts.Scanned--
				continue
			}
			fail(ingest.CodeReadDenied)
			report.Counts.Failed++
			report.Items = append(report.Items, watchItem{Path: abs, State: "failed",
				Code: string(ingest.CodeReadDenied), Detail: err.Error()})
			continue
		}

		var rec watchRecord
		found, err := ingest.LoadState(w.stateDir, abs, &rec)
		if err != nil && !errors.Is(err, ingest.ErrCorruptState) {
			fail(ingest.CodeStateCorrupt)
			report.Counts.Failed++
			report.Items = append(report.Items, watchItem{Path: abs, State: "failed",
				Code: string(ingest.CodeStateCorrupt), Detail: err.Error()})
			continue
		}
		if errors.Is(err, ingest.ErrCorruptState) {
			// A record that does not decode is treated as no record, exactly as the
			// transcript sink treats a broken cursor: broken state must not block
			// ingestion. The cycle says so instead of pretending it knew.
			fail(ingest.CodeStateCorrupt)
			found = false
		}

		switch {
		case !found:
			// First sighting. Deferring by one interval is what makes append-only
			// safe: a half-written file frozen into a content-addressed row can
			// never be repaired by this tier, because the completed file is a
			// "change", and changes are not re-ingested.
			w.observe(abs, size, mtime, fail)

		case rec.IngestedAt == "":
			if rec.Size != size || rec.ModTime != mtime {
				// Still moving. Re-arm the quiet-interval gate.
				w.observe(abs, size, mtime, fail)
				continue
			}
			attempts++
			item, code, err := w.upload(ctx, abs)
			if err != nil {
				fail(code)
				report.Counts.Failed++
				report.Items = append(report.Items, watchItem{Path: abs, State: "failed",
					Code: string(code), Detail: err.Error()})
				continue
			}
			if serr := ingest.SaveState(w.stateDir, abs, watchRecord{
				Abs: abs, Size: size, ModTime: mtime,
				SHA256: item.sha, FileID: item.FileID, VirtualPath: item.VirtualPath,
				IngestedAt: w.now().UTC().Format(time.RFC3339Nano),
			}); serr != nil {
				// The content is in the vault but its local record did not persist.
				// Say so as a failure: the next cycle may re-offer the file, and the
				// server will answer 秒传, so the risk is a duplicate report, not a
				// lost object.
				fail(ingest.CodeStateCorrupt)
				report.Counts.Failed++
				report.Items = append(report.Items, watchItem{Path: abs, State: "failed",
					Code: string(ingest.CodeStateCorrupt), FileID: item.FileID, VirtualPath: item.VirtualPath,
					Detail: "uploaded, but its local record could not be saved: " + serr.Error()})
				continue
			}
			if item.State == "deduped" {
				report.Counts.Deduped++
			} else {
				report.Counts.Ingested++
			}
			report.Items = append(report.Items, item.watchItem)

		default:
			if rec.Size == size && rec.ModTime == mtime {
				// The cost gate's whole purpose: an unchanged file is never read
				// and never hashed.
				report.Counts.Unchanged++
				continue
			}
			// Stat moved, content may not have. rsync -t, touch, backup restores
			// and cloud-sync placeholders all move mtime without moving bytes, so
			// mtime alone must never re-ingest: sha256 is the authority.
			sum, herr := ingest.ContentHash(abs)
			if herr != nil {
				code := ingest.Classify(herr)
				fail(code)
				report.Counts.Failed++
				report.Items = append(report.Items, watchItem{Path: abs, State: "failed",
					Code: string(code), Detail: herr.Error()})
				continue
			}
			if sum == rec.SHA256 {
				rec.Size, rec.ModTime = size, mtime
				if serr := ingest.SaveState(w.stateDir, abs, rec); serr != nil {
					fail(ingest.CodeStateCorrupt)
					report.Counts.Failed++
					continue
				}
				report.Counts.Unchanged++
				continue
			}
			// A real change: reported, never re-ingested. The record stays pointed
			// at what is actually in the vault, so the next report can name the id
			// this file is a candidate replacement for.
			report.Counts.Changed++
			report.Items = append(report.Items, watchItem{
				Path: abs, State: "changed",
				FileID: rec.FileID, VirtualPath: rec.VirtualPath, SHA256: shortHash(rec.SHA256),
				Detail: "content differs from the ingested version; not re-ingested (the file plane has no revision model)",
			})
		}
	}

	// Records whose file is gone. Reported, never deleted remotely: a local
	// deletion does not propagate (REQ-003), and the record is kept, so a file
	// that comes back is still recognized as the one already in the vault.
	for _, gone := range w.vanishedRecords(paths) {
		report.Counts.LocalGone++
		report.Items = append(report.Items, watchItem{
			Path: gone.Abs, State: "local_gone",
			FileID: gone.FileID, VirtualPath: gone.VirtualPath, SHA256: shortHash(gone.SHA256),
			Detail: "no longer on disk; the stored copy is untouched",
		})
	}

	return w.finish(report, started), attempts, nil
}

// observe records a first sighting or a refreshed stability observation. It is
// not an outcome, so it adds no count; a save failure is a real failure and is
// reported as one.
func (w *watchRun) observe(abs string, size int64, mtime string, fail func(ingest.Code)) {
	if err := ingest.SaveState(w.stateDir, abs, watchRecord{Abs: abs, Size: size, ModTime: mtime}); err != nil {
		fail(ingest.CodeStateCorrupt)
	}
}

func (w *watchRun) finish(r watchCycle, started time.Time) watchCycle {
	r.DurationMS = w.now().Sub(started).Milliseconds()
	if len(r.Failures) == 0 {
		r.Failures = nil
	}
	return r
}

// vanishedRecords returns this root's records whose local file is no longer
// there. Reading the store in bulk is only ever needed by a watcher, so it lives
// here rather than in the core.
func (w *watchRun) vanishedRecords(present []string) []watchRecord {
	entries, err := os.ReadDir(w.stateDir)
	if err != nil {
		return nil
	}
	live := make(map[string]bool, len(present))
	for _, p := range present {
		live[p] = true
	}
	var out []watchRecord
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		var rec watchRecord
		b, rerr := os.ReadFile(filepath.Join(w.stateDir, e.Name()))
		if rerr != nil {
			// An unreadable record is not evidence that its file vanished. The
			// per-file path reports it as state_corrupt on the next pass.
			continue
		}
		if err := json.Unmarshal(b, &rec); err != nil || rec.Abs == "" {
			continue
		}
		if live[rec.Abs] {
			continue
		}
		if _, err := os.Stat(rec.Abs); err == nil {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Abs < out[j].Abs })
	return out
}

// uploadResult keeps the reported hash prefix beside the full hash the record
// needs, so the log stays readable without losing the authority value.
type uploadResult struct {
	watchItem
	sha string
}

// upload pushes one file through the same multipart endpoint `mem put` uses.
func (w *watchRun) upload(ctx context.Context, abs string) (uploadResult, ingest.Code, error) {
	rel, err := filepath.Rel(w.root, abs)
	if err != nil {
		return uploadResult{}, ingest.CodeReadDenied, err
	}
	folder := w.to
	if d := filepath.Dir(rel); d != "." {
		folder = joinFolder(w.to, filepath.ToSlash(d))
	}

	// Hashed before the upload, from the bytes this cycle verified as quiet: the
	// record must describe what was sent, not what the file may have become while
	// it was in flight.
	sum, err := ingest.ContentHash(abs)
	if err != nil {
		return uploadResult{}, ingest.Classify(err), err
	}

	f, err := os.Open(abs)
	if err != nil {
		return uploadResult{}, ingest.Classify(err), err
	}
	defer f.Close()

	name := filepath.Base(abs)
	mimeType := mime.TypeByExtension(filepath.Ext(name))
	var resp struct {
		File struct {
			ID   string `json:"id"`
			Path string `json:"path"`
		} `json:"file"`
		Deduped bool `json:"deduped"`
	}
	if err := w.client.api.UploadMultipartWithSourceMetadata(ctx, name, mimeType, folder, f, w.tags, w.sourceMeta, &resp); err != nil {
		// Deliberately the apiclient call rather than the httpClient wrapper: the
		// wrapper maps errors into cliError for one-shot commands, and the shared
		// classifier needs the raw *apiclient.APIError to name the code. This is
		// the same seam the transcript sink uses for the same reason.
		code := ingest.Classify(err)
		if ctx.Err() != nil && code == ingest.CodeNetwork {
			// The run was stopped while this upload streamed. The transport says
			// "network"; what happened is that this cycle ran out of room, and
			// naming the exit-code-owning surface's truth is this file's job.
			code = ingest.CodeProviderTimeout
		}
		return uploadResult{}, code, err
	}
	state := "ingested"
	if resp.Deduped {
		// Content already in the vault is a success, and it is a 秒传. Counting it
		// as ingested would overstate what this run wrote.
		state = "deduped"
	}
	return uploadResult{
		sha: sum,
		watchItem: watchItem{
			Path: abs, State: state,
			FileID: resp.File.ID, VirtualPath: resp.File.Path, SHA256: shortHash(sum),
		},
	}, "", nil
}

// shortHash keeps a locator readable in a terminal. The full hash stays in the
// record, which is the durable authority.
func shortHash(sum string) string {
	if len(sum) <= 12 {
		return sum
	}
	return sum[:12]
}

// diagf writes a human line that is not part of the cycle stream. In json mode
// stdout carries report lines only, so diagnostics move to stderr; in text mode
// the whole surface is human and stays on stdout.
func (w *watchRun) diagf(format string, args ...any) {
	out := w.cmd.OutOrStdout()
	if w.format == "json" {
		out = w.cmd.ErrOrStderr()
	}
	fmt.Fprintf(out, format, args...)
}

// print emits one cycle. JSON mode writes exactly one line per cycle, byte-type
// identical to what was appended to the log, so a supervisor's stdout can be
// piped into any JSON-lines consumer.
func (w *watchRun) print(r watchCycle) {
	out := w.cmd.OutOrStdout()
	if w.format == "json" {
		enc := json.NewEncoder(out)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(r); err != nil {
			w.diagf("warn: report encode: %v\n", err)
		}
		return
	}
	fmt.Fprintf(out, "cycle %d  %s -> %s  (%dms)\n", r.Cycle, r.Root, r.To, r.DurationMS)
	fmt.Fprintf(out, "  scanned %d  ingested %d  deduped %d  unchanged %d  changed %d  local_gone %d  failed %d\n",
		r.Counts.Scanned, r.Counts.Ingested, r.Counts.Deduped, r.Counts.Unchanged,
		r.Counts.Changed, r.Counts.LocalGone, r.Counts.Failed)
	for _, it := range r.Items {
		switch it.State {
		case "failed":
			fmt.Fprintf(out, "  ! %s: %s: %s\n", it.Path, it.Code, it.Detail)
		case "changed":
			fmt.Fprintf(out, "  ~ %s: changed since %s (ingested as %s); not re-ingested\n",
				it.Path, it.SHA256, it.FileID)
		case "local_gone":
			fmt.Fprintf(out, "  - %s: gone locally; stored copy %s untouched\n", it.Path, it.FileID)
		default:
			fmt.Fprintf(out, "  + %s: %s -> %s\n", it.Path, it.State, it.VirtualPath)
		}
	}
}

// persist appends the cycle to the JSONL log, then caps the log. The cap is
// applied after the append so rotation can never drop the newest entry.
func (w *watchRun) persist(r watchCycle) error {
	if err := os.MkdirAll(filepath.Dir(w.reportLog), 0o700); err != nil {
		return err
	}
	line, err := json.Marshal(r)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(w.reportLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return rotateReportLog(w.reportLog)
}

func rotateReportLog(path string) error {
	lines, err := readTail(path, watchReportLogCap)
	if err != nil {
		return err
	}
	if len(lines) < watchReportLogCap {
		return nil
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// readTail returns the last n lines of a file, oldest first.
func readTail(path string, n int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		lines = append(lines, sc.Text())
		if len(lines) > n {
			lines = lines[len(lines)-n:]
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

// allFailuresNonRetryable reports whether every failure this cycle saw belongs
// to the class D-1 says a watcher must not ride out: a rejected credential, a
// refused plan or quota, or an upstream that will not answer. Anything else (a
// network blip, one rejected file) is worth another cycle.
func allFailuresNonRetryable(r watchCycle) bool {
	if len(r.Failures) == 0 {
		return false
	}
	for code := range r.Failures {
		switch code {
		case string(ingest.CodeAuth), string(ingest.CodePlanQuota), string(ingest.CodeProviderTimeout):
			continue
		default:
			return false
		}
	}
	return true
}

// giveUpExit maps the give-up class onto its SPEC §7.1 process code. The order is
// fixed rather than a map walk, so a cycle that mixed two of these classes always
// reports the same code to the supervisor that restarts it.
func giveUpExit(r watchCycle) int {
	for _, code := range []string{
		string(ingest.CodeAuth), string(ingest.CodePlanQuota), string(ingest.CodeProviderTimeout),
	} {
		if r.Failures[code] > 0 {
			switch code {
			case string(ingest.CodeAuth):
				return 3
			case string(ingest.CodePlanQuota):
				return 4
			}
			return 5
		}
	}
	return 5
}

// bindWatchFlags adds the SPEC.md:604 flag and the one tuning knob D-1
// adjudicated. Watching is recursive by definition, so --recursive stays
// unrelated to it.
func bindWatchFlags(cmd *cobra.Command, watch *bool, interval *time.Duration) {
	cmd.Flags().BoolVar(watch, "watch", false, "keep running and ingest new files found under a directory (one-way, local→mem only)")
	cmd.Flags().DurationVar(interval, "interval", watchIntervalDefault, "poll interval for --watch; a file is ingested once it has been quiet for one interval")
}
