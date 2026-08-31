# Directory watch (`mem put --watch`)

`mem put <dir> --watch` is the file-plane counterpart of `mem ingest qoder`: a
foreground watcher that keeps one local directory in sync **in one direction**
with a mem folder. It polls the tree, uploads files it has not seen yet, and never
touches what already landed — no delete, no overwrite, no re-upload of a changed
file. It is a `put`, not a sync client, and it is deliberately not a daemon: run
it under systemd, `docker stop`, `tmux` or your shell, and it stops when you stop
it.

## Usage

```
mem auth login                                  # once, if not already logged in
mem put ~/Downloads/inbox --watch --to /Inbox   # poll every 30s
mem put ~/Photos --watch --to /Photos/2026 --interval 5m --tag from-camera
mem put ~/inbox --watch --format json           # one JSON report line per cycle
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--watch` | `false` | keep running and ingest new files found under the directory |
| `--interval` | `30s` | poll cadence; a file is ingested once it has been quiet for one full interval |
| `--to` | `/` | destination folder; subdirectories map under it (see below) |
| `--tag` | — | attached to every file in the run (repeatable) |
| `--format` | `text` | `json` makes stdout a pure JSON-lines report stream |

The source-metadata flags (`--captured-at`, `--lat`, `--lon`,
`--location-accuracy`, `--place`, `--source-kind`, `--source-name`) are accepted
and applied to every file the run uploads. `--name` and `--mime` are rejected,
because each file keeps its own name and type. `--recursive` is unnecessary —
watching is recursive — and stdin is rejected, because a pipe is a one-shot
source.

## What gets ingested, and when

The watcher's unit is the file, and its authority is the file's content hash:

| Observed state | Report | Action |
| --- | --- | --- |
| first sighting | `scanned` only, in that cycle | record `(size, mtime)`; ingest in the **next** cycle if the file has not moved |
| `size`/`mtime` unchanged | `unchanged` | nothing — the file is not opened and not hashed |
| `mtime` moved, content identical | `unchanged` | refresh the record; no upload. `rsync -t`, `touch`, backup restores and cloud-sync placeholders all move mtime without moving bytes |
| content differs from the ingested version | `changed` | reported with the `file_id` it is a candidate replacement for, **never** re-ingested — the file plane has no revision model |
| file gone, record kept | `local_gone` | reported with the stored `file_id`; nothing is deleted remotely, and a file that comes back is still recognized as the one already in the vault |
| server already holds these bytes | `deduped` | the upload was a replay (`200` + `deduped`), counted separately from `ingested` |
| upload refused | `failed` + a failure code | the record is not advanced, so the next cycle retries |

The one-interval delay on a first sighting is what makes append-only sources safe:
a half-written file frozen into a content-addressed copy can never be repaired by
this tier, because a completed file then looks like a `change`, and changes are
not re-ingested. Waiting for one quiet interval keeps the ingested copy of a
slowly-written export whole.

Destination folders follow the same rule as `mem put <dir> --recursive`: a file at
`<root>/photos/2012/x.jpg` with `--to /Photos` lands at `/Photos/photos/2012/x.jpg`.

## One-way by contract

Per REQ-003, local deletions and local edits do not propagate. The watcher issues
`POST /v1/files` and nothing else — no `DELETE`, no `PATCH`, and no write-back to
the watched tree. The test suite asserts this at the HTTP layer: every request the
watcher makes is recorded, and a handler that sees anything other than
`POST /v1/files` fails the run.

## State, reports and the lock

All paths are under the CLI state root (`~/.mem`, relocated by `MEM_STATE_DIR`),
keyed by the SHA-1 of the absolute watched root:

| Path | Contents |
| --- | --- |
| `~/.mem/watch/<sha1(root)>/cursors/<sha1(abs)>.json` | one record per file: size, mtime, sha256, the returned `file_id` and virtual path, and when it was ingested |
| `~/.mem/watch/reports/<sha1(root)>.jsonl` | the last 200 cycle reports, newest last |
| `~/.mem/watch/locks/<sha1(root)>.lock` | advisory lock, one watcher per root |

Records are written atomically (temp file + rename) and share the core's keying, so
a run interrupted mid-write leaves a readable record, not a half-written one.
Deleting the state tree is safe: the watcher re-offers every file, and the server's
content-addressed dedup turns those into `deduped` reports rather than duplicates.

The lock is `flock` on unix, so the kernel releases it when the holder exits for
any reason — a crashed watcher needs no operator cleanup, and the lock file itself
stays in place. On platforms without advisory locks (Windows) exclusivity is
claimed by creating the file, and a killed holder leaves it behind; the error names
the path so an operator can remove it.

## Exit codes

Exit codes follow SPEC §7.1, like every other command:

| Code | When |
| --- | --- |
| `0` | stopped: `SIGINT` (`Ctrl-C`) or `SIGTERM` arrived between cycles |
| `1` | another watcher already holds this root, or the flag combination is invalid (`--name`/`--mime`/`--interval 0`/stdin) |
| `2` | the watch root does not exist, or is a file rather than a directory |
| `3` | gave up on authentication refusals (401/403) |
| `4` | gave up on plan or quota blocks (402/429) |
| `5` | gave up on provider or timeout failures (502/503/504) |

A missing root *after* startup is reported as `root_missing` each cycle rather than
exiting, because an unmounted volume may come back. The give-up bound exists so a
watcher cannot spin silently on a broken credential: 10 consecutive cycles in which
every upload attempt failed for a non-retryable reason exit with that class's code.
Transient classes — `network`, and a single file the server refused
(`upload_rejected`) — never trigger it; those are retried until you stop the run.

## Cycle report

Each cycle is one JSON object, on stdout in `--format json` and in the report log
either way:

```json
{"contract":"mem.put-watch","schema_version":1,"cycle":2,"root":"/home/me/inbox",
 "to":"/Inbox","started_at":"2026-08-31T06:14:16.765Z","duration_ms":3,
 "counts":{"scanned":2,"ingested":2,"deduped":0,"unchanged":0,"changed":0,"local_gone":0,"failed":0},
 "items":[{"path":"/home/me/inbox/a.txt","state":"ingested","file_id":"fid-001",
           "virtual_path":"/Inbox/a.txt","sha256":"2c8b08da5ce6"}]}
```

`counts` is a closed set — exactly those seven keys, always all present — and
`failures` maps a closed code set to counts: `auth`, `plan_quota`,
`provider_timeout`, `network`, `read_denied`, `upload_rejected`, `root_missing`,
`state_corrupt`. `contract` and `schema_version` are the version handle a log
consumer keys on, so a future report shape is an additive decision rather than a
silent break.

## Privacy note

The watcher uploads the full bytes of every file under the root you point at, and
`--to` decides who can reach them afterwards. Pick a root you would be willing to
have searched later; `--interval` and a first `--to /Drafts` pass are cheaper ways
to find out than a broad root followed by selective `forget`.
