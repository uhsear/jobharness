# jobharness

One import gives an unattended script logging, retry, resume, a safe unzip, and an FTPS client
that talks to servers which require TLS session reuse.

Scripts that run on a scheduler all need the same seven things, and every script re-derives them
slightly differently. Measured directly across the one production tree that motivated this file:
26 files with their own logging setup and 37 handler sites between them, three drifting retention
policies (prune by count, prune by age, prune never), 5 unguarded `extractall` calls, and retry
written three different ways. jobharness is those seven things written down once, as thin wrappers
over the standard library.

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
jobharness self-test: 118 assertions passed
```

That run is offline: no network, no credentials, no config, and nothing written outside a temp
directory it deletes on the way out. The assertions are behavioral, not smoke. Swap `safe_extract`
for a plain `zipfile.extractall` and the run stops at assertion 18, `safe_extract refuses ../
traversal`.

## Install

One file, standard library only, Python 3.8+, nothing to pip install.

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
from datetime import datetime
from jobharness import setup_logging, safe_extract, Checkpoints, run_summary

started = datetime.now()
log, log_path = setup_logging("nightly", "logs", retain_count=14)
cp = Checkpoints("logs/nightly.json", logger=log)
if not cp.is_done("unpack"):
    cp.mark_done("unpack", files=len(safe_extract("incoming/data.zip", "work/data")))
log.info(run_summary("nightly", started, steps=cp.state["completed_steps"]))
```

## What is in it

Seven helpers, ranked by how often the corpus needed them. `setup_logging` is first because 26
files needed it; `FTPSession` is last because one did. The order is corpus frequency, not value:
`FTPSession` is the one people arrive here looking for. Each entry names the stdlib call it wraps,
because that is all any of them is.

### 1. `setup_logging(name, log_dir, retain_days=None, retain_count=None, level=logging.INFO, console=True, now=None)`

Wraps `logging.FileHandler` plus `logging.StreamHandler`, then an `os.listdir` / `os.remove` purge.
Returns `(logger, log_path)`, the file named `<name>_YYYYmmdd_HHMMSS.log`.

Deleting files is **off by default**. Pass exactly one of `retain_days` or `retain_count`; passing
both raises `ValueError`, since a fleet of scripts sharing one policy is the whole point. Pruning
only ever considers files matching that filename pattern in `log_dir`, and never the log the call
just opened, so another job's logs, a hand-written `notes.log`, and unrelated files are all safe.
Pass `now` to pin the age cutoff to one clock reading; mtime exactly at the cutoff is kept.

### 2. `safe_extract(zip_path, dest)`

Wraps `zipfile.ZipFile.extract` behind an `os.path.realpath` + `os.path.commonpath` containment
check. Returns the extracted file paths, raises `UnsafeArchiveError` naming the offending member.

Refuses absolute paths, drive-letter paths, `..` traversal on any host OS, two members resolving to
one file, and any member that resolves through a symlink or junction to outside `dest`. Containment
is decided on `realpath`, not `abspath`, because abspath is string math and cannot see a link.
Every member is validated before a byte is written, so a refusal leaves nothing on disk and does
not even create `dest`. Honest note: `ZipFile.extract` already sanitizes these names silently. This
refuses loudly instead, so a tampered archive is a visible failure in the job log.

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

### 5. `Checkpoints(path, logger=None)`

Wraps `json` plus an atomic temp-file `os.replace`. Methods: `is_done(step)`,
`mark_done(step, **data)`, `get(step, default=None)`, `save()`, `clear()`.

A crash mid-write cannot leave a half-file. A state file that is corrupt, unreadable, or the wrong
shape is reported as a warning and treated as "nothing done yet", because a resume aid must never
be the thing that stops the job.

### 6. `FTPSession()`

Wraps `ftplib.FTP_TLS` with two overrides, `ntransfercmd` and `makepasv`. Adopted, not invented;
see Credit.

`ntransfercmd` reuses the control channel's TLS session on the data channel. A server that enforces
session reuse - vsftpd `require_ssl_reuse`, IIS FTP - rejects a data connection that does not, and
vsftpd answers `522 SSL connection failed; session reuse required`. `ftplib.FTP_TLS.ntransfercmd`
wraps the data socket with `server_hostname` only and passes no `session`, so stock ftplib cannot
satisfy those servers. That defect is still open upstream: CPython bpo-19500 / gh-63699.

`makepasv` returns the host you already reached instead of the private address a NAT'd server puts
in its PASV reply. That half is vestigial on a current interpreter: since CPython bpo-43285,
backported to 3.6.14, 3.7.11, 3.8.9, 3.9.3 and 3.10, `ftplib` defaults
`trust_server_pasv_ipv4_address` to `False` and substitutes `self.sock.getpeername()[0]` itself, so
`super().makepasv()` has already discarded the server-returned host before the override returns.
It is kept because an interpreter older than that fix still trusts the PASV reply, and deleting it
buys nothing.

### 7. `run_summary(name, started, finished=None, steps=None, errors=None, extra=None, width=68)`

Wraps `str.join` and `datetime.timedelta`. Pure formatting: writes nothing, logs nothing, returns a
block for the tail of a job log.

## Credit

`FTPSession` is adopted, not original. Both overrides are the widely-copied public workaround
circulated on Stack Overflow and the CPython issue tracker (bpo-19500 / gh-63699) for two interop
problems: FTPS servers that require TLS session reuse on the data channel (vsftpd
`require_ssl_reuse`, IIS FTP), and NAT'd servers that answer PASV with a private address. Credit to
the original authors. It is reproduced here only so scripts stop pasting it.

`safe_extract` implements the standard zip-slip guard, the same containment check written up in
Python's own `zipfile` security notes and in every writeup of the pattern. Nothing about the
approach is new here.

## Limitations

1. **True FTPS session reuse is not asserted by the self-test.** That needs a real server. The
   self-test proves both methods are overridden, that constructing `FTPSession` opens no socket,
   and that `makepasv` returns the original host against a stub. Resumption itself is unclaimed.
   On an interpreter carrying the bpo-43285 fix, only the `ntransfercmd` half changes behavior.
2. **`Checkpoints` is generalized from a single site.** Unlike the other six, this pattern had
   exactly one prior production use, not a repeated one. It is the thinnest evidence in the module.
3. **This is a convenience library, not novel work.** Every helper is a thin wrapper you could
   write yourself. The value is that it is written once, tested once, and identical everywhere.
4. **No zip-bomb defence.** `safe_extract` checks where members land, not their unpacked size.
5. **No log rotation mid-run.** One file per run, pruned at startup. For size-based rotation,
   `logging.handlers.RotatingFileHandler` is already in the stdlib and better at it.
6. **No thread safety.** `Checkpoints` assumes one writer; two concurrent runs against one state
   file will lose writes.
7. **No scheduler and no DAG.** `Checkpoints` records what finished; the `if` statement is yours.

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
