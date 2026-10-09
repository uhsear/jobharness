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

jobharness answers both. `RunLock` refuses a dead run's lock with exit code 4 instead of exiting 0,
and never deletes it on its own. `Checkpoints.step()` records each step as ok, skipped or failed.
A step that logged an ERROR is failed. A step whose input files changed runs again.

Scripts that run on a scheduler all need the same handful of things, and every script re-derives
them slightly differently. Measured directly across the one production tree that motivated this
file: 26 files with their own logging setup and 37 handler sites between them, three drifting
retention policies (prune by count, prune by age, prune never), 5 unguarded `extractall` calls,
and retry written three different ways. jobharness is those things written down once, as thin
wrappers over the standard library.

The high-water mark for not having `setup_logging` is one file in that tree: a 10.5 MB, 287,171-line
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
PASS  a step that logged an ERROR and returned is FAILED, not ok  <-- pinned defect
...
PASS  a changed input makes an ok step not done, so it runs again  <-- pinned defect
...
PASS  a skipped step is not done, so a resume runs it again  <-- pinned defect
...
PASS  the same run key resumes, and a resume tells ok from skipped from failed
...
PASS  a lock whose pid is gone from this host is stale  <-- pinned defect
...
PASS  a dead run's lock is refused with exit code 4, not skipped  <-- pinned defect
PASS  and the error says how to check and remove it
PASS  acquiring never deletes a stale lock itself  <-- pinned defect
PASS  the exists-then-exit-0 guard reports success on the same dead lock
...
PASS  a unique prefix --ap of --apply is REFUSED by the parser  <-- pinned defect
...
PASS  check() and raises() really do record a failure  <-- pinned defect
PASS  caught() returns None when nothing is raised
--------------------------------------------------------------------
259 assertions, 0 failed
```

The full run prints all 259 assertions. Each `...` above is where this block is cut. The same 259
assertions, with the same labels, pass on Windows (Python 3.13.2) and on Ubuntu (Python 3.12.3).

That run is offline: no network, no credentials, no config, and nothing written outside a temp
directory it deletes on the way out. It starts two short-lived child Python processes to test the
pid probe against a real live process and a real exited one. The assertions are behavioral, not
smoke. Swap `safe_extract` for a plain `zipfile.extractall` and 23 assertions fail, the first one
`safe_extract refuses ../ traversal`. Make `lock_verdict` ignore a dead pid and 7 fail. Let a
skipped step count as done and `a skipped step is not done, so a resume runs it again` fails.

## Requirements

Python 3.9 or newer, standard library only, nothing to pip install. The self-test has been run on
3.12.3 and 3.13.2. The code uses no syntax newer than 3.9, but 3.12 is the oldest interpreter it
has been run on.

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
stale. It reads only. `--apply` removes the lock, and only when the lock is stale.

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
| `--lock PATH` | none | Report whether the run lock at PATH is free, held or stale. Reads only. |
| `--max-age SECONDS` | none | A lock older than this is stale even if its pid is alive. Needed to judge a lock written on another host. |
| `--apply` | off | With `--lock`: remove the lock if, and only if, it is stale. |
| `--self-test` | off | Run the offline assertions and exit. |

| Exit code | Meaning |
|---|---|
| 0 | No lock file, or a stale lock was removed under `--apply` |
| 2 | Usage error. A prefix such as `--ap` is refused, because the parser sets `allow_abbrev=False`. |
| 3 | Held: a live run owns the lock. `--apply` never removes a held lock. |
| 4 | Stale: the run that wrote the lock is gone, or the lock is older than `--max-age` |

`--apply` reads the lock a second time just before it deletes it. If the file changed between the
two reads, nothing is removed.

## What is in it

Eight helpers. The first seven are ranked by how often the corpus needed them: `setup_logging` is
first because 26 files needed it, `FTPSession` sixth because one did. `RunLock` is eighth because
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
  ERROR or CRITICAL on `logger` or its child loggers while the body runs. An `ErrorCounter`
  handler watches the logger for the length of the body and is removed afterwards. A record
  logged with the wrong format arguments still counts.
- **Skipped** if the body called `run.skip("reason")`. A skip with no reason is a `ValueError`,
  because a skip with no reason reads as a success.
- **Ok** otherwise. `run.data` is saved with the step.

Only an ok step is done. A resume runs a skipped or failed step again, and a step that was ok and
then fails is no longer done.

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
`acquire()` raises `LockError`, whose `exit_code` is 3 when a live run holds the lock and 4 when
it is stale. `release()` deletes the file only if it still holds this run's token.

The verdict on an existing lock, from `lock_verdict()`, a pure function:

- **Same host.** The pid is probed. A gone pid is stale. A live pid is held, unless the lock is
  older than `max_age`, which makes it stale: the run is hung, or its pid was reused.
- **Another host.** Its pids cannot be probed from here, so only age decides. Without `max_age`
  the lock is held. An age exactly equal to `max_age` is still held.
- **No readable record.** Held for its first 10 seconds, because a starting run may still be
  writing it. Stale after that. A lock file that cannot be opened at all is held.

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
   run so two runs never share one state file.
7. **No scheduler and no DAG.** `Checkpoints` records what happened; the `if` statement is yours.
8. **A reused pid reads as held.** If the operating system gives a dead run's pid to an unrelated
   process on the same host, the lock reads as held until `max_age` passes. Set `max_age`.
9. **Exit code 259 on Windows.** A process that exited with code 259 reads as alive, as Microsoft
   documents for `GetExitCodeProcess`.
10. **A lock from another host is judged by age only.** Without `max_age` it stays held.
11. **`O_EXCL` on network shares.** On an NFS mount older than NFSv3, or on any share that does
    not honour exclusive create, two runs can both acquire the lock. The second read in
    `--apply` narrows the window between check and delete, but does not close it.
12. **`ErrorCounter` sees one logger tree.** It counts records on the logger passed to `step()` and
    its child loggers. A record on an unrelated logger, or one filtered out by a logger level
    above ERROR, is not counted.
13. **Fingerprints read every byte.** Each fingerprint reads each input in full. For very large
    inputs that is a real cost; the alternative, size and mtime, misses a same-size rewrite.
14. **Coverage.** Branch coverage of `jobharness.py --self-test` on Windows is 99.56%. Two places
    are not reached. The symlink fallback in the self-test runs only on a host that cannot create
    symlinks, and both test hosts can. The `if __name__ == "__main__":` line is never false
    when the file runs as a script. The library code itself is fully covered on Windows. On Linux
    the Windows `kernel32` adapter is not reached, and on Windows the real POSIX `os.kill` call is
    not made. The probe logic of both runs on both hosts against fakes.

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
