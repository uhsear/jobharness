# jobharness

One import gives an unattended script logging, retry, resume, a run lock, a safe unzip, and an
FTPS client that talks to servers which require TLS session reuse.

Here is how the usual overlap guard fails. A nightly job writes `nightly.lock` when it starts and
deletes it when it ends. If the lock is already there, the job logs "already running" and exits 0.
Then one night the server reboots in the middle of a run. The lock is never deleted. Every run
after that finds it, logs "already running", and exits 0. The scheduler shows a green tick every
morning, and nothing has run since the reboot.

The same job resumes from a checkpoint file. Its geocode step catches its own exceptions, logs
them at ERROR and returns, and the step is marked done at the end. A night when half the rows fail
to geocode is recorded exactly like a clean night, so the resume skips that step from then on.
Nothing in the file can tell ok from skipped from failed, or a changed input from a re-run.

jobharness answers both. `RunLock` raises `LockError` for a dead run's lock instead of letting the
job exit 0, and never deletes the lock on its own. The error carries `exit_code` 4. The exit code
comes from your script, which passes it to `sys.exit(exc.exit_code)`, or from the `--lock` check
below. An uncaught `LockError` ends the script with exit code 1. `Checkpoints.step()` records each
step as ok, skipped or failed. A step that logs an ERROR is failed, if that ERROR reaches a
logger the step watches. With no `logger` argument that is the root logger, so any logger that
propagates to the root counts. A step whose input files changed runs again.

Scripts that run on a scheduler all need the same handful of things, and every script re-derives
them slightly differently. Measured directly across the one production tree that motivated this
file (the counts are rounded): about 25 files with their own logging setup and about 40 handler
sites between them, three drifting retention policies (prune by count, prune by age, prune never),
a handful of unguarded `extractall` calls, and retry written several different ways. jobharness is
those things written down once, as thin wrappers over the standard library.

The high-water mark for not having `setup_logging` is one file in that tree: a 287,171-line
interactive console transcript, saved beside the scripts with a `.py` extension. Scanned end to end
for dates and clock times, one line in 287,171 carries anything time-shaped, and that line is the
interpreter's own startup banner. Not one line of output can be attributed to a run, a step, or an
hour.

**Looking for the FTPS `522 SSL connection failed; session reuse required` fix?** It is
[`FTPSession`](#6-ftpsession) below, ten lines: the `ntransfercmd` override that passes
`session=self.sock.session` when it wraps the data socket, for servers that enforce TLS session
reuse (vsftpd `require_ssl_reuse`, IIS FTP). CPython bpo-19500 / gh-63699, still open. Import the
class or copy it. Adopted, not invented - see [Credit](#credit).

```
$ python jobharness.py --self-test
jobharness self-test: no network, no credentials, temp files only
--------------------------------------------------------------------
PASS  retry re-raises after exhaustion
PASS  retry re-raises the LAST exception, not the first
...
PASS  a host that cannot make symlinks SKIPS the three containment checks, it does not pass them  <-- pinned defect
...
PASS  a step that logged an ERROR and returned is FAILED, not ok  <-- pinned defect
...
PASS  a changed input makes an ok step not done, so it runs again  <-- pinned defect
...
PASS  with no logger given, an ERROR on the job's own logger fails the step  <-- pinned defect
...
PASS  a skipped step is not done, so a resume runs it again  <-- pinned defect
...
PASS  a date and a Path in run.data are saved as text, not a TypeError  <-- pinned defect
...
PASS  token=, user:pass@, Authorization and apikey= values never reach the state file  <-- pinned defect
...
PASS  a lock whose pid is gone from this host is stale  <-- pinned defect
...
PASS  text that is not a lock record is NOTLOCK however old, never stale  <-- pinned defect
...
PASS  a dead run's lock is refused with exit code 4, not skipped  <-- pinned defect
...
PASS  acquiring never deletes a stale lock itself  <-- pinned defect
...
PASS  --lock --apply on a data file exits 5 and removes nothing  <-- pinned defect
...
PASS  a unique prefix --ap of --apply is REFUSED by the parser  <-- pinned defect
...
PASS  an empty --lock path ['--lock', ''] is a usage error, not exit 0  <-- pinned defect
...
PASS  check() and raises() really do record a failure  <-- pinned defect
PASS  a skipped check is listed in the summary and is not counted as a pass  <-- pinned defect
PASS  caught() returns None when nothing is raised
--------------------------------------------------------------------
291 assertions, 0 failed
```

The full run prints all 291 assertions. Each `...` above is where this block is cut. The same 291
assertions, with identical labels, pass on Windows (Python 3.13.2, 3.12 and 3.9.25) and on Ubuntu
(Python 3.12.3). The two outputs were diffed and are the same text.

That run is offline: no network, no credentials, no config, and nothing written outside a temp
directory it deletes on the way out. It starts a few short-lived child Python processes to test the
pid probe against a real live process and a real exited one. The assertions are behavioral, not
smoke. Swap `safe_extract` for a body that calls `zipfile.ZipFile.extractall` and 24 assertions
fail, the first one `safe_extract refuses ../ traversal`. The count depends on how the swap is
written. Make `lock_verdict` ignore a dead pid and 7 assertions report FAIL, then the run stops on
an uncaught `LockError` before it prints a footer (Limitation 25). Let a skipped step count as done
and 5 fail, the first `a step that skipped is skipped`.

One assertion is an illustration and not a test of this module. `the exists-then-exit-0 guard
reports success on the same dead lock` runs a four-line stand-in for the old guard, to show what
the old guard did. It cannot fail.

If a host cannot create symlinks (Windows without Developer Mode or administrator rights), the
three symlink containment checks are printed as `SKIP`, are not counted as assertions, and are
listed under the footer. The count on that host is then 288 and not 291. Exit code stays 0, so read
the footer. A clean run on such a host has not shown that `safe_extract` follows symlinks safely.

Branch coverage of `jobharness.py --self-test` is 100% on Windows (`coverage run --branch`, 1481
statements, 238 branches). One exclusion is named in the code: the `if __name__` line carries `# pragma: no branch`, because
the false side is an import and the measured run is a script. The symlink fallback is reached by
passing the checks a function that cannot create a link.

## Requirements

Python 3.9 or newer, standard library only, nothing to pip install. The self-test has been run on
3.9.25, 3.12 and 3.13.2 on Windows and on 3.12.3 on Ubuntu, with the same 291 assertions each time.
3.9.25 is the oldest interpreter it has been run on, and the code uses no syntax newer than 3.9.

```
git clone https://github.com/uhsear/jobharness.git
cd jobharness
```

Or copy `jobharness.py` next to your script. Import time touches no filesystem, network, or clock.

## Quick start

```
python jobharness.py --self-test
```

```python
import sys
from datetime import date, datetime
from jobharness import setup_logging, safe_extract, Checkpoints, RunLock, LockError, run_summary

started = datetime.now()
log, log_path = setup_logging("nightly", "logs", retain_count=14)
try:
    with RunLock("logs/nightly.lock", max_age=6 * 3600):
        cp = Checkpoints("logs/nightly.json", logger=log, run_key=date.today().isoformat())
        if not cp.is_done("unpack", inputs="incoming/data.zip"):
            with cp.step("unpack", inputs="incoming/data.zip") as run:
                run.data["files"] = len(safe_extract("incoming/data.zip", "work/data"))
        log.info("unpack is %s", cp.status("unpack"))
        log.info(run_summary("nightly", started, steps=cp.state["completed_steps"]))
except LockError as exc:
    log.error("%s", exc)
    sys.exit(exc.exit_code)
```

This example was run as written on both hosts, against a synthetic two-file zip. The first run
unpacks. The second run resumes and skips the step. After a third file is added to the zip, the
next run logs `step unpack: its inputs changed since it ran, so it runs again`. With a lock left
by an exited process, the script exits 4 and logs the stale verdict.

## Usage: checking a lock

The command line has one job besides `--self-test`: say whether a `RunLock` file is free, held or
stale. It reads only. `--apply` removes the lock, and only when the lock is stale. The `--apply`
gate covers this command line. The library call `setup_logging(..., retain_days=...)` deletes old
logs when you call it, and Limitation 20 says so.

```
$ python jobharness.py --lock nightly.lock
HELD: pid 5120 on job-host, started 2026-10-08T23:00:00, 8:00:00 ago: a pid on another host cannot be checked from here; pass a max age to judge it by age
$ echo $?
3
$ python jobharness.py --lock nightly.lock --max-age 21600
STALE: pid 5120 on job-host, started 2026-10-08T23:00:00, 8:00:00 ago: older than the 21600s max age, so the run is hung or its pid was reused
Nothing was removed. Re-run with --apply to remove this stale lock.
$ echo $?
4
$ python jobharness.py --lock nightly.lock --max-age 21600 --apply
STALE: pid 5120 on job-host, started 2026-10-08T23:00:00, 8:00:00 ago: older than the 21600s max age, so the run is hung or its pid was reused
Removed nightly.lock.
$ echo $?
0
```

Those three runs used a synthetic lock file written for a host that is not this one.

| Flag | Default | What it does |
|---|---|---|
| `--lock PATH` | none | Report whether the run lock at PATH is free, held, stale or not a lock. Reads only. An empty PATH is a usage error. |
| `--max-age SECONDS` | none | A lock older than this is stale even if its pid is alive. Needed to judge a lock written on another host. |
| `--apply` | off | With `--lock`: remove the lock if, and only if, it is stale. |
| `--self-test` | off | Run the offline assertions and exit. |

| Exit code | Meaning |
|---|---|
| 0 | No lock file, or a stale lock was removed under `--apply` |
| 2 | Usage error. A prefix such as `--ap` is refused, because the parser sets `allow_abbrev=False`. |
| 3 | Held: a live run owns the lock. `--apply` never removes a held lock. |
| 4 | Stale: the run that wrote the lock is gone, or the lock is older than `--max-age` |
| 5 | Not a lock: the file holds text that is not a lock record. `--apply` never removes it. |

`--apply` reads the lock a second time just before it deletes it. If the file changed between the
two reads, nothing is removed.

## What is in it

Eight helpers. The first seven are ranked by how often the corpus needed them: `setup_logging` is
first because about 25 files needed it, `FTPSession` sixth because one did. `RunLock` is eighth because
it has no corpus count behind it at all. It answers a failure mode, not a repeated pattern. Each
entry names the stdlib call it wraps.

### 1. `setup_logging(name, log_dir, retain_days=None, retain_count=None, level=logging.INFO, console=True, now=None)`

Wraps `logging.FileHandler` plus `logging.StreamHandler`, then an `os.listdir` / `os.remove` purge.
Returns `(logger, log_path)`, the file named `<name>_YYYYmmdd_HHMMSS.log`.

Deleting files is **off by default**. Pass exactly one of `retain_days` or `retain_count`; passing
both raises `ValueError`, since a fleet of scripts sharing one policy is the whole point. Pruning
only ever considers files matching that filename pattern in `log_dir`, and never the log the call
just opened, so another job's logs, a hand-written `notes.log`, and unrelated files are all safe.
Pass `now` to pin the age cutoff to one clock reading; mtime exactly at the cutoff is kept. A
second call for the same name replaces the handlers it installed and keeps any handler it did not.
A log that cannot be deleted is logged as a warning and kept.

### 2. `safe_extract(zip_path, dest)`

Wraps `zipfile.ZipFile.extract` behind an `os.path.realpath` + `os.path.commonpath` containment
check. Returns the extracted file paths, raises `UnsafeArchiveError` naming the offending member.

Refuses absolute paths, drive-letter paths, `..` traversal on any host OS, two members resolving to
one file, and any member that resolves through a symlink or junction to outside `dest`. Containment
is decided on `realpath`, not `abspath`, because abspath is string math and cannot see a link.
Every member is validated before a byte is written, so a refusal leaves nothing on disk and does
not even create `dest`. If a write fails part way, what the call wrote is removed, and a `dest`
that already existed keeps its other files. Honest note: `ZipFile.extract` already sanitizes these
names silently. This refuses loudly instead, so a tampered archive is a visible failure in the job
log.

### 3. `retry(exceptions, attempts=3, delay=1.0, backoff=2.0, on_retry=None, sleep=time.sleep)`

Wraps `functools.wraps` and `time.sleep`.

Sleeps `delay`, `delay*backoff`, `delay*backoff**2`, never after the final attempt. Re-raises the
**last** exception with its original traceback, not the first. Unlisted exceptions propagate
immediately. `on_retry(exc, attempt)` gets a 1-based number, and `sleep` is injectable so the
backoff schedule is assertable without a real clock.

### 4. `EmailNotifier(host, port=25, user=None, password_env=None, use_tls=False, timeout=30, smtp_factory=smtplib.SMTP, logger=None)`

Wraps `smtplib.SMTP` and `email.message.EmailMessage`.

`password_env` is the **name** of an environment variable, never a literal. The value is read at
connect time, held only as a local, and rebound in a `finally`, so it is not on the instance and
cannot reach a `repr`, a log record, or `connect`'s own traceback frame even when the login raises.
An unset variable fails loudly. `send` catches `smtplib.SMTPServerDisconnected`, reconnects exactly
once, and resends once, which is what a job idle between scheduler runs actually needs.
`smtp_factory` is injectable, which is how the self-test covers the whole path without a socket.

### 5. `Checkpoints(path, logger=None, run_key=None)`

Wraps `json` plus an atomic temp-file `os.replace`, and `hashlib.sha256` for input fingerprints.

A crash mid-write cannot leave a half-file. A state file that is corrupt, unreadable, or the wrong
shape is reported as a warning and treated as "nothing done yet", because a resume aid must never
be the thing that stops the job. A file written before step states existed still reads: a step in
its `completed_steps` list is ok.

| Method | What it does |
|---|---|
| `step(name, logger=None, inputs=None)` | Context manager. Runs the body and records the step as ok, skipped or failed. |
| `is_done(step, inputs=None)` | True only for an ok step. With `inputs`, also only if their fingerprint is unchanged. |
| `status(step)` / `reason(step)` | `'ok'`, `'skipped'`, `'failed'` or `None`, and why it was skipped or failed. |
| `record(step, state, reason=None, inputs=None, **data)` | Record a state directly. Skipped and failed need a reason. |
| `mark_done(step, **data)`, `get(step)`, `save()`, `clear()` | As before: record ok, read step data, write, forget. |

Inside `step()`:

- **Failed** if the body raises (the exception still propagates), or if anything is logged at
  ERROR or CRITICAL on the watched logger or its child loggers while the body runs. An
  `ErrorCounter` handler (a `logging.Handler` that keeps the message of every ERROR or CRITICAL
  record, with a `level` argument that defaults to `logging.ERROR`) watches the logger for the
  length of the body and is removed afterwards. A record logged with the wrong format arguments
  still counts. The watched logger is `step(logger=...)`, else the `logger` given to
  `Checkpoints(...)`, else the ROOT logger. With no logger at all, an ERROR from a logger that
  propagates to the root is counted.
- **Skipped** if the body called `run.skip("reason")`. A skip with no reason is a `ValueError`,
  because a skip with no reason reads as a success.
- **Ok** otherwise. `run.data` is saved with the step. A value that JSON cannot encode, such as a
  `Path` or a `datetime`, is saved as its `str()` text.

Only an ok step is done. A resume runs a skipped or failed step again, and a step that was ok and
then fails is no longer done.

If the state file cannot be written while the body is already raising, the body's own exception
reaches the caller and the write error is logged. The temp file is removed on any write failure.

**Secrets in a reason.** The reason of a failed step is copied from an exception or an ERROR log
message, and lands in the state file. `redact()` first blanks the value in `token=`, `password=`,
`api_key=` and similar parameters, in `Authorization: Bearer ...`, and the password in
`https://user:password@host`. The patterns are a net, not a guarantee, and `run.data` is not
redacted. The self-test writes synthetic secrets of each kind and checks that none reaches the file.

**Run key.** Pass a string such as the date a daily job is for. A state file written under a
different key is ignored and the run starts fresh, with an INFO line naming the old key. The same
key resumes. With no run key the file is always resumed, as before.

**Input fingerprints.** `fingerprint(paths)` is a SHA-256 over the path, size and bytes of every
input file. Listing order does not matter. Which name holds which bytes does, so two inputs that
swap contents change it. `step()` takes the fingerprint **before** the body runs, so an input
edited while the step reads it is caught on the next run instead of being recorded as the version
the step used. A missing input fails the step before its body runs.

### 6. `FTPSession()`

Wraps `ftplib.FTP_TLS` with two overrides, `ntransfercmd` and `makepasv`. Adopted, not invented;
see Credit.

`ntransfercmd` reuses the control channel's TLS session on the data channel. A server that enforces
session reuse - vsftpd `require_ssl_reuse`, IIS FTP - rejects a data connection that does not, and
vsftpd answers `522 SSL connection failed; session reuse required`. `ftplib.FTP_TLS.ntransfercmd`
wraps the data socket with `server_hostname` only and passes no `session`, so stock ftplib cannot
satisfy those servers. That defect is still open upstream: CPython bpo-19500 / gh-63699. The
self-test calls both the stock method and the override against a fake TLS context, and asserts
that only the override passes `session`.

`makepasv` returns the host you already reached instead of the private address a NAT'd server puts
in its PASV reply. That half is vestigial on a current interpreter: since CPython bpo-43285,
backported to 3.6.14, 3.7.11, 3.8.9, 3.9.3 and 3.10, `ftplib` defaults
`trust_server_pasv_ipv4_address` to `False` and substitutes `self.sock.getpeername()[0]` itself, so
`super().makepasv()` has already discarded the server-returned host before the override returns.
It is kept because an interpreter older than that fix still trusts the PASV reply, and deleting it
buys nothing.

### 7. `run_summary(name, started, finished=None, steps=None, errors=None, extra=None, width=68)`

Pure string formatting: writes nothing, logs nothing, returns a block for the tail of a job log.

### 8. `RunLock(path, max_age=None, logger=None)`

Wraps `os.open(path, O_CREAT | O_EXCL | O_WRONLY)`. Use it as `with RunLock(...):` or call
`acquire()` and `release()`.

`acquire()` creates the lock file atomically: of two runs racing for it, exactly one succeeds. It
writes the pid, the host name, the start time and a random token. If the file already exists,
`acquire()` raises `LockError`, whose `exit_code` is 3 when a live run holds the lock, 4 when
it is stale and 5 when the file is not a lock record at all. The exit code reaches the scheduler
only if your script calls `sys.exit(exc.exit_code)`, as the Quick start does. `release()` deletes
the file only if it still holds this run's token.

The verdict on an existing lock, from `lock_verdict()`, a pure function:

- **Same host.** The pid is probed. A gone pid is stale. A live pid is held, unless the lock is
  older than `max_age`, which makes it stale: the run is hung, or its pid was reused.
- **Another host.** Its pids cannot be probed from here, so only age decides. Without `max_age`
  the lock is held. An age exactly equal to `max_age` is still held.
- **Empty file.** Held for its first 10 seconds, because a starting run may still be writing it.
  Stale after that.
- **Text that is not a lock record.** Not a lock, however old (exit code 5). It is somebody's
  data, such as a CSV or a checkpoint file reached through a mistyped path. `--apply` never
  removes it. A lock file that cannot be opened at all is held.

`acquire()` never deletes a lock, stale or not. Two runs that each judged the same lock stale and
each deleted it would both run. Removing a stale lock is a separate step that a person takes:
`python jobharness.py --lock PATH --apply`.

The pid probe is `kill(pid, 0)` on POSIX, where ESRCH means gone and EPERM means alive but owned by
another user. It is never `os.kill` on Windows. There, signal 0 is `CTRL_C_EVENT`, and any other
value terminates the process. On Windows the probe calls `OpenProcess` and then
`GetExitCodeProcess`. Access denied means alive. A process that can still be opened but has an exit
code other than `STILL_ACTIVE` is gone. A process object stays open while any handle to it exists,
so a successful open does not by itself mean the process is running.

## The existing alternatives

- **[filelock](https://py-filelock.readthedocs.io/)** is the mature choice for mutual exclusion.
  Its default `FileLock` uses an OS lock (`fcntl.flock` on Unix, `LockFileEx` on Windows), and its
  documentation says the Unix lock is released automatically on a crash, so it cannot go stale on
  the same host. Its `SoftFileLock` is a marker file with same-host pid inspection, and it can
  reclaim a provably-gone owner. If you can add a dependency and need only exclusion, use it.
  `RunLock` differs in that it never reclaims a lock by itself, it judges a lock from another host
  by age, and it gives a scheduler distinct exit codes for held and stale.
- **`flock -n` (util-linux)** runs a command under an OS lock that is dropped when the file is
  closed, and exits 1 by default (`-E` changes it) when the lock is taken. On Linux, for a
  shell-level guard, it is simpler than any of this. It is not on Windows.
- **[doit](https://pydoit.org/)** is a task runner that tracks `file_dep` by MD5 and skips a task
  whose dependencies did not change. That is the same idea as `is_done(step, inputs=...)`, inside
  a full build tool. Its documentation says it saves the MD5 after the actions run, and warns that
  editing a dependency while a task runs can store a checksum the task did not use. `step()`
  fingerprints before the body for that reason.

A `gh search repos jobharness` found one other public repo with this name,
[Inayat-0007/jobharness](https://github.com/Inayat-0007/jobharness), a job-posting scraper. It is
unrelated: a different kind of job, and not an equivalent of this tool.

## Credit

`FTPSession` is adopted, not original. Both overrides are the widely-copied public workaround
circulated on Stack Overflow and the CPython issue tracker (bpo-19500 / gh-63699) for two interop
problems: FTPS servers that require TLS session reuse on the data channel (vsftpd
`require_ssl_reuse`, IIS FTP), and NAT'd servers that answer PASV with a private address. Credit to
the original authors. It is reproduced here only so scripts stop pasting it.

`safe_extract` implements the standard zip-slip guard, the same containment check written up in
Python's own `zipfile` security notes and in every writeup of the pattern. Nothing about the
approach is new here.

## References

The operating-system behaviour `RunLock` relies on, checked against the official documentation:

- [open(2)](https://man7.org/linux/man-pages/man2/open.2.html): with `O_CREAT | O_EXCL`, "if
  pathname already exists, then open() fails with the error EEXIST". On NFS, `O_EXCL` is supported
  only on NFSv3 or later with kernel 2.6 or later.
- [_open](https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/open-wopen): with
  `_O_CREAT | _O_EXCL`, `_open` "returns an error value if a file specified by filename exists",
  with `errno` `EEXIST`.
- [kill(2)](https://man7.org/linux/man-pages/man2/kill.2.html): with signal 0 "no signal is sent,
  but existence and permission checks are still performed". `ESRCH`: the process does not exist.
  `EPERM`: no permission to signal it.
- [Python os.kill](https://docs.python.org/3/library/os.html#os.kill): on Windows, any signal other
  than `CTRL_C_EVENT` and `CTRL_BREAK_EVENT` kills the process with `TerminateProcess`.
- [OpenProcess](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-openprocess):
  returns NULL on failure, with the reason from `GetLastError`. Some system processes always
  return `ERROR_ACCESS_DENIED`.
- [GetExitCodeProcess](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getexitcodeprocess):
  returns `STILL_ACTIVE` (259) while the process has not terminated, and warns that a process
  which exits with code 259 is indistinguishable from a running one.

## Limitations

1. **True FTPS session reuse is not asserted against a server.** That needs a real server. The
   self-test proves that the override hands the control session to the data socket and the stock
   method does not, against a fake TLS context, and that `makepasv` returns the original host
   against a stub. Resumption itself is unclaimed.
2. **`Checkpoints` is generalized from a single site.** Unlike the other corpus-ranked helpers,
   this pattern had exactly one prior production use, not a repeated one. The step states, run key and
   fingerprints come from the failure it showed, not from a count.
3. **This is a convenience library, not novel work.** Every helper is a thin wrapper you could
   write yourself. The value is that it is written once, tested once, and identical everywhere.
4. **No zip-bomb defence.** `safe_extract` checks where members land, not their unpacked size.
5. **No log rotation mid-run.** One file per run, pruned at startup. For size-based rotation,
   `logging.handlers.RotatingFileHandler` is already in the stdlib and better at it.
6. **One writer per state file.** `Checkpoints` assumes one writer. Hold a `RunLock` around the
   run so two runs never share one state file. The temp file is always `<path>.tmp`, so two
   writers would also collide on it.
7. **No scheduler and no DAG.** `Checkpoints` records what happened; the `if` statement is yours.
8. **A reused pid reads as held.** If the operating system gives a dead run's pid to an unrelated
   process on the same host, the lock reads as held until `max_age` passes. Set `max_age`.
9. **Exit code 259 on Windows.** A process that exited with code 259 reads as alive, as Microsoft
   documents for `GetExitCodeProcess`.
10. **A lock from another host is judged by age only.** Without `max_age` it stays held.
11. **`O_EXCL` on network shares.** On an NFS mount older than NFSv3, or on any share that does
    not honour exclusive create, two runs can both acquire the lock. The second read in
    `--apply` narrows the window between check and delete, but does not close it.
12. **`ErrorCounter` sees one logger tree.** It counts records on the watched logger and its
    child loggers: the `logger` passed to `step()`, else the one passed to `Checkpoints`, else the
    root logger. With a logger given, an ERROR on an unrelated logger or on the root is not
    counted. A record filtered out by a logger level above ERROR, or dropped by
    `logging.disable()`, is not counted. A `logging.LoggerAdapter` has no `addHandler`, so pass
    the underlying `Logger`.
13. **Fingerprints read every byte.** Each fingerprint reads each input in full. For very large
    inputs that is a real cost; the alternative, size and mtime, misses a same-size rewrite. The
    path is hashed as you give it, so the same bytes reached by another relative or absolute path
    read as changed and the step runs again.
14. **Coverage.** Branch coverage of `jobharness.py --self-test` on Windows is 100%. The `if
    __name__ == "__main__":` line is excluded with `# pragma: no branch`, because its false side
    is an import. On Linux the Windows `kernel32` adapter is not reached, and on Windows the real
    POSIX `os.kill` call is not made. The probe logic of both runs on both hosts against fakes,
    so the POSIX probe has met a real process only on Ubuntu, and the `kernel32` probe only on
    Windows. Coverage was measured on Windows.
15. **A hand-edited lock time can break a verdict.** A lock file whose `ts` is NaN, an infinity,
    or more than about 8.6e13 seconds in the past makes the verdict code raise `ValueError` or `OverflowError` instead
    of returning a verdict, so the check ends with a traceback and exit code 1.
16. **`inputs` must be files.** A directory path raises `PermissionError` on Windows or
    `IsADirectoryError` on Linux when the step starts, and `is_done` returns False for it every
    time, so the step never resumes.
17. **`get(step)` can return old data.** It keeps the data of an earlier ok run when a later run
    of the same step fails, or finishes ok without setting `run.data`, so it can return stale
    values for a step whose status is `failed`.
18. **`record()` accepts a reason of only spaces** for skipped and failed, although
    `run.skip()` rejects one.
19. **A hand-edited state file is not repaired.** A `data` value that is not a dict, a step record
    that is not a dict, or a step in `completed_steps` whose `steps` entry says failed, makes a
    later write raise, or makes `is_done` return True while `status` says failed. Delete the file
    to start fresh. A file saved with a UTF-8 BOM, for example by Windows PowerShell 5
    `Set-Content -Encoding utf8`, is read as unreadable and the run starts fresh.
20. **Only the command line is gated by `--apply`.** `setup_logging` with `retain_days` or
    `retain_count` deletes old logs when you call it. Log file names have a resolution of one
    second, so two runs of one job in the same second share one log file.
21. **A write failure outside an error.** If the body succeeds and the state file cannot be
    written (disk full), the write error is raised from the `with` block and the in-memory status
    already says ok. Keep `run.data` small and let the job fail loudly.
22. **An empty lock file older than 10 seconds is called stale.** On a network share whose clock is
    more than 10 seconds behind this host, a run that is still writing its lock can read as stale.
23. **Containers and host names.** Under Docker the host name changes with each container, so a
    lock left by an earlier container reads as another host and stays held until `--max-age`
    passes. Always set `max_age` there. A process that has exited but is not yet reaped still
    answers `kill(pid, 0)` on POSIX, so its lock reads as held until its parent reaps it or
    `max_age` passes.
24. **`safe_extract` duplicate and rollback edge cases.** Duplicate members are found by
    normcased `realpath`, so on Windows two members that `zipfile` rewrites to the same name (for
    example `a:b` and `a_b`) can overwrite each other without a refusal. That was reasoned from
    the code and not run. A failed extraction into an existing directory also removes a file of
    the same name that the call overwrote, because the rollback deletes every path the call wrote.
25. **A broken check can stop the self-test.** If a bug makes the self-test raise instead of
    fail an assertion (for example a `lock_verdict` that ignores a dead pid), it prints a
    traceback and exits 1 with no summary line.
26. **Mail and FTPS security defaults.** `EmailNotifier` does not refuse a login on an unencrypted
    connection: set `use_tls=True` whenever you pass a `user` and `password_env`. Its
    `starttls()` call and `FTPSession` use Python's default TLS context, which on Python 3.13
    does not check the server certificate (`FTPSession().context.verify_mode` is `CERT_NONE`).
    `FTPSession` accepts `context=ssl.create_default_context()`, and `EmailNotifier` has no way to
    pass one. `EmailNotifier.connect()` drops its SMTP object without closing it when the login fails.
27. **`--lock` exit codes for unreadable files.** A lock file that exists but cannot be read, such as
    a directory path or a permission error, exits 3, the same as a live run. Read the message to
    tell them apart.
28. **`--apply` can end in a traceback.** If it cannot delete the lock (a permission error, or a
    Windows file held open), it ends with a traceback and exit code 1.
29. **An unmounted share looks free.** `--lock` reports no lock, with exit code 0, for a path under
    a share that is not mounted. `RunLock.acquire()` then creates the directory locally, so the
    lock is not shared between hosts.
30. **`release()` is check then remove.** It reads the token and then deletes the file in two calls.
    A lock that another run takes between the two calls can be deleted. The window is small.
31. **The default logger has no handler.** With no `logger` argument, `Checkpoints` and `RunLock`
    log to `logging.getLogger("jobharness")`. Warnings such as `lock now belongs to another run`
    then reach only Python's last-resort handler on stderr. Pass a logger.
32. **`--lock` prints the lock file's text as written.** Do not point it at a file you do not trust,
    because a hand-made lock file can carry terminal escape sequences into your console.

## Contributing

Issues and pull requests welcome. The self-test must stay offline, credential-free, and green.

## Author

Built by [Asir Khan](https://www.linkedin.com/in/asir-khan-310317264/).

## License

MIT.

## Related

Other single-file tools in this portfolio that pair with this one:

- [taskpulse](https://github.com/uhsear/taskpulse) - find the scheduled tasks that are silently failing
- [svcguard](https://github.com/uhsear/svcguard) - keep a service up around the risky part of the job
- [logsift](https://github.com/uhsear/logsift) - read the logs back out once the harness is writing them
