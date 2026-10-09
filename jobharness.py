#!/usr/bin/env python3
"""jobharness - one import gives a scheduled script logging, retry, resume, a run lock and a safe unzip.

A convenience layer over the standard library for Python scripts that run
unattended on a scheduler (cron, Windows Task Scheduler, systemd timers).
Nothing here is novel. Each helper is a thin, tested wrapper over a stdlib
call, written down once so a fleet of scripts stops re-deriving it:

    setup_logging   -> logging.FileHandler + logging.StreamHandler + glob/unlink purge
    safe_extract    -> zipfile.ZipFile.extract behind a resolved-path containment check
    retry           -> functools.wraps + time.sleep
    EmailNotifier   -> smtplib.SMTP
    Checkpoints     -> json, with ok / skipped / failed step states, a run key,
                       input fingerprints (hashlib) and an ERROR-log handler
    FTPSession      -> ftplib.FTP_TLS  (adopted public workaround, see class docstring)
    run_summary     -> string formatting
    RunLock         -> os.open(O_CREAT | O_EXCL); a stale lock is refused, never ignored

Python 3.9+. Standard library only, no optional third-party imports.
Import time touches nothing: no filesystem, no network, no clock.

Run the offline self-test:

    python jobharness.py --self-test

Check a run lock (reads only). Exit 0 no lock, 3 held, 4 stale:

    python jobharness.py --lock nightly.lock --max-age 21600

Remove the lock only if it is stale:

    python jobharness.py --lock nightly.lock --apply
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import ftplib
import functools
import hashlib
import json
import logging
import os
import re
import shutil
import smtplib
import socket
import sys
import time
import zipfile
from email.message import EmailMessage

__version__ = "1.1.0"
__all__ = [
    "setup_logging",
    "safe_extract",
    "UnsafeArchiveError",
    "retry",
    "EmailNotifier",
    "Checkpoints",
    "StepRun",
    "ErrorCounter",
    "fingerprint",
    "step_outcome",
    "FTPSession",
    "run_summary",
    "RunLock",
    "LockError",
    "inspect_lock",
    "lock_verdict",
    "parse_lock",
    "pid_alive",
]

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
_TIMESTAMP_FMT = "%Y%m%d_%H%M%S"

# Marks handlers this module installed, so a second setup_logging() call for the
# same name replaces them instead of stacking a duplicate console handler.
_OWNED = "_jobharness_owned"


# ---------------------------------------------------------------------------
# 1. Logging: one retention policy, replacing prune-by-count / prune-by-age / never
# ---------------------------------------------------------------------------

def _log_pattern(name):
    """Regex matching only the log files setup_logging(name, ...) creates."""
    return re.compile(r"^" + re.escape(name) + r"_\d{8}_\d{6}\.log$")


def _prune_logs(log_dir, name, retain_days, retain_count, keep, logger, now_ts):
    """Delete this job's own old log files. Never touches anything else.

    `keep` is the path of the log currently open; it is never a deletion
    candidate even if a policy would otherwise select it. `now_ts` is the epoch
    seconds the age cutoff is measured from. One clock reading, shared with
    the log filename's timestamp, so the boundary is exact and testable.
    A file whose mtime is exactly the cutoff is KEPT; only strictly older goes.
    """
    pattern = _log_pattern(name)
    keep = os.path.abspath(keep)
    candidates = []
    try:
        entries = os.listdir(log_dir)
    except OSError:
        return []
    for entry in entries:
        if not pattern.match(entry):
            continue
        path = os.path.abspath(os.path.join(log_dir, entry))
        if path == keep:
            continue
        try:
            if not os.path.isfile(path):
                continue
            candidates.append((os.path.getmtime(path), entry, path))
        except OSError:
            continue

    if retain_days is not None:
        cutoff = now_ts - retain_days * 86400.0
        doomed = [c for c in candidates if c[0] < cutoff]
    else:
        # retain_count includes the log just opened, which is always newest.
        candidates.sort()  # (mtime, name) ascending == oldest first
        surplus = len(candidates) + 1 - retain_count
        doomed = candidates[:surplus] if surplus > 0 else []

    removed = []
    for _mtime, _entry, path in doomed:
        try:
            os.remove(path)
            removed.append(path)
        except OSError as exc:  # locked by another run, permissions, etc.
            logger.warning("could not prune %s: %s", path, exc)
    return removed


def setup_logging(name, log_dir, retain_days=None, retain_count=None,
                  level=logging.INFO, console=True, now=None):
    """Configure a named logger with a timestamped file plus a console handler.

    Wraps logging.FileHandler / logging.StreamHandler, then optionally purges
    old logs with a glob-and-unlink.

    Deleting files is OFF by default. Pass exactly one of:
        retain_days  - delete this job's logs whose mtime is older than N days
        retain_count - keep the N newest of this job's logs (including the new one)
    Passing both is a ValueError; the whole point is that a fleet of scripts
    should share one policy, not three.

    Only files matching ``<name>_YYYYmmdd_HHMMSS.log`` in `log_dir` are ever
    considered for deletion, and never the log this call just opened.

    Returns (logger, log_path).
    """
    if retain_days is not None and retain_count is not None:
        raise ValueError("choose one retention policy: retain_days or retain_count, not both")
    if retain_days is not None and retain_days <= 0:
        raise ValueError("retain_days must be > 0 (use None to disable pruning)")
    if retain_count is not None and retain_count < 1:
        raise ValueError("retain_count must be >= 1 (use None to disable pruning)")
    if not name:
        raise ValueError("name is required")

    os.makedirs(log_dir, exist_ok=True)
    started = now or _dt.datetime.now()
    log_path = os.path.join(log_dir, "%s_%s.log" % (name, started.strftime(_TIMESTAMP_FMT)))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # our handlers are the whole story; no root duplication
    for handler in list(logger.handlers):
        if getattr(handler, _OWNED, False):
            logger.removeHandler(handler)
            handler.close()

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    setattr(file_handler, _OWNED, True)
    logger.addHandler(file_handler)

    if console:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        setattr(stream_handler, _OWNED, True)
        logger.addHandler(stream_handler)

    if retain_days is not None or retain_count is not None:
        _prune_logs(log_dir, name, retain_days, retain_count, log_path, logger,
                    started.timestamp())

    return logger, log_path


# ---------------------------------------------------------------------------
# 2. safe_extract: zipfile.extractall behind a containment check
# ---------------------------------------------------------------------------

class UnsafeArchiveError(Exception):
    """A zip member would be written outside the destination directory."""


_DRIVE = re.compile(r"^[A-Za-z]:")


def _reject_reason(member_name):
    """Return a string reason this member is unsafe, or None if it is fine."""
    if not member_name or member_name in (".", ".."):
        return "empty or dot-only member name"
    if member_name.startswith("/") or member_name.startswith("\\"):
        return "absolute path"
    if _DRIVE.match(member_name):
        return "drive-letter path"
    parts = re.split(r"[\\/]", member_name)
    if ".." in parts:
        return "parent-directory traversal"
    return None


def safe_extract(zip_path, dest):
    """Extract `zip_path` into `dest`, refusing any member that escapes it.

    Refused: absolute paths, Windows drive-letter paths, ``..`` traversal (on
    any host OS), and two members that resolve to the same file. Every member is
    checked before a single byte is written, so a refusal leaves nothing on disk
    , and `dest` is not even created. If a write fails partway through anyway (a
    member used as both a file and a directory, permissions, a full disk), what
    this call wrote is removed before the error propagates.

    Containment is decided on ``os.path.realpath``, not ``os.path.abspath``:
    abspath is pure string math, so a member with a perfectly clean name still
    lands outside `dest` when `dest` contains a symlink or an NTFS junction
    pointing elsewhere. realpath resolves those links even for a leaf that does
    not exist yet.

    Honest note: zipfile.ZipFile.extract already *sanitizes* these names
    silently (it strips the drive, leading separators, and ``..`` components).
    This function refuses loudly instead, and names the offending member, so a
    tampered archive is a visible failure in the job log rather than a file
    quietly landing somewhere you did not ask for.

    Returns the list of extracted file paths (directories excluded).
    """
    dest_real = os.path.realpath(dest)
    with zipfile.ZipFile(zip_path, "r") as archive:
        members = archive.infolist()

        seen = {}
        for member in members:
            reason = _reject_reason(member.filename)
            if reason is None:
                target = os.path.realpath(os.path.join(dest_real, member.filename))
                try:
                    contained = os.path.commonpath([dest_real, target]) == dest_real
                except ValueError:  # different drives on Windows
                    contained = False
                if not contained:
                    reason = "resolves outside destination"
                else:
                    # normcase, so a case-only duplicate is caught on the
                    # filesystems where it is a silent overwrite.
                    key = os.path.normcase(target)
                    if key in seen and not member.is_dir():
                        reason = "collides with earlier member %r" % (seen[key],)
                    seen[key] = member.filename
            if reason is not None:
                raise UnsafeArchiveError(
                    "refusing %r from %s: %s" % (member.filename, zip_path, reason)
                )

        pre_existing = os.path.isdir(dest_real)
        os.makedirs(dest_real, exist_ok=True)
        extracted = []
        written = []
        try:
            for member in members:
                path = archive.extract(member, dest_real)
                written.append(path)
                if not member.is_dir():
                    extracted.append(path)
        except Exception:
            _undo_extract(written, dest_real, pre_existing)
            raise
    return extracted


def _undo_extract(written, dest_real, pre_existing):
    """Best-effort removal of what one failed safe_extract call wrote."""
    for path in reversed(written):
        try:
            if os.path.isdir(path):
                os.rmdir(path)
            else:
                os.remove(path)
        except OSError:
            pass
    if not pre_existing:
        # archive.extract() also creates intermediate directories that are not
        # in `written`; dest did not exist before this call, so it all goes.
        shutil.rmtree(dest_real, ignore_errors=True)


# ---------------------------------------------------------------------------
# 3. retry decorator
# ---------------------------------------------------------------------------

def retry(exceptions, attempts=3, delay=1.0, backoff=2.0, on_retry=None, sleep=time.sleep):
    """Retry the wrapped call on `exceptions` with exponential backoff.

    Wraps functools.wraps + time.sleep. Sleeps `delay`, then `delay * backoff`,
    then `delay * backoff**2`, ... between attempts. Never sleeps after the
    final attempt. When attempts are exhausted the LAST exception is re-raised,
    with its original traceback.

    Exceptions not listed in `exceptions` propagate immediately, with no retry.

    on_retry(exc, attempt) is called after each failed-but-will-retry attempt,
    with `attempt` 1-based. `sleep` is injectable so the backoff schedule can be
    asserted without a real clock.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt >= attempts:
                        raise
                    if on_retry is not None:
                        on_retry(exc, attempt)
                    sleep(delay * (backoff ** (attempt - 1)))
                    attempt += 1
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# 4. EmailNotifier
# ---------------------------------------------------------------------------

class EmailNotifier:
    """Send a plain-text notification over SMTP, reconnecting once if dropped.

    Wraps smtplib.SMTP. The password is read from the environment variable
    *named* by `password_env` at connect time and held only as a local. It is
    never stored on the instance, so it cannot reach a repr, a traceback frame
    of this object, or a log record, including when the login itself fails.
    Pass a variable NAME, never a literal. (smtplib's own ``SMTP.login`` frame
    still holds it while it runs; that frame is smtplib's, not ours.)

    A long-idle scheduled job routinely finds its SMTP socket closed by the
    server. `send` catches smtplib.SMTPServerDisconnected, reconnects exactly
    once, and retries the message once.

    `smtp_factory` is injectable so the whole path can be tested without a
    network call.
    """

    def __init__(self, host, port=25, user=None, password_env=None, use_tls=False,
                 timeout=30, smtp_factory=smtplib.SMTP, logger=None):
        self.host = host
        self.port = port
        self.user = user
        self.password_env = password_env
        self.use_tls = use_tls
        self.timeout = timeout
        self._smtp_factory = smtp_factory
        self._log = logger or logging.getLogger(__name__)
        self._smtp = None

    def __repr__(self):
        return "EmailNotifier(host=%r, port=%r, user=%r, password_env=%r, use_tls=%r)" % (
            self.host, self.port, self.user, self.password_env, self.use_tls
        )

    def connect(self):
        smtp = self._smtp_factory(self.host, self.port, timeout=self.timeout)
        smtp.ehlo()
        if self.use_tls:
            smtp.starttls()
            smtp.ehlo()
        if self.user and self.password_env:
            password = os.environ.get(self.password_env)
            if not password:
                raise RuntimeError(
                    "environment variable %r is unset or empty" % (self.password_env,)
                )
            try:
                smtp.login(self.user, password)
            finally:
                # NOT `del password` after the call: a login that raises never
                # reaches it, and this frame is then attached to the caller's
                # traceback with the plaintext still bound. Rebinding in
                # `finally` scrubs it on the failure path too.
                password = None
        self._smtp = smtp
        return smtp

    def send(self, subject, body, to, from_addr):
        """Send one message. Returns True on success; reconnects at most once."""
        recipients = [to] if isinstance(to, str) else [a for a in to if a]
        if not recipients:
            raise ValueError("at least one recipient is required")

        msg = EmailMessage()
        msg["From"] = from_addr
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.set_content(body)
        raw = msg.as_string()

        if self._smtp is None:
            self.connect()
        try:
            self._smtp.sendmail(from_addr, recipients, raw)
        except smtplib.SMTPServerDisconnected:
            self._log.warning("SMTP server disconnected; reconnecting once")
            self.close()
            self.connect()
            self._smtp.sendmail(from_addr, recipients, raw)
        return True

    def close(self):
        if self._smtp is not None:
            try:
                self._smtp.quit()
            except Exception:  # already dead; nothing useful to do
                pass
            self._smtp = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


# ---------------------------------------------------------------------------
# 5. Checkpoints: step states ok / skipped / failed, a run key, input
#    fingerprints, and an ERROR-log handler
# ---------------------------------------------------------------------------

OK = "ok"
SKIPPED = "skipped"
FAILED = "failed"
STATES = (OK, SKIPPED, FAILED)


def fingerprint(paths):
    """Return 'sha256:<hex>' over the path, size and bytes of every input file.

    `paths` is one path or a list. The order the caller lists them in does not
    matter (they are sorted), but which name holds which bytes does: the path
    is hashed with its content, so two inputs that swap contents change the
    fingerprint. Each part is length-prefixed, so ('ab', '') and ('a', 'b')
    cannot collide. A missing or unreadable input raises OSError.
    """
    if isinstance(paths, (str, os.PathLike)):
        paths = [paths]
    digest = hashlib.sha256()
    for path in sorted(os.fspath(p) for p in paths):
        name = path.encode("utf-8", "surrogateescape")
        digest.update(b"%d:" % len(name) + name)
        digest.update(b"%d:" % os.path.getsize(path))
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
    return "sha256:" + digest.hexdigest()


def step_outcome(raised, skip_reason, errors):
    """Decide one step's (state, reason). Pure: no I/O.

    An exception beats everything. An ERROR record logged during the step beats
    a skip: a step that logged an error and carried on did not succeed, which is
    the case a plain `mark_done` at the end of the step records as done.
    """
    if raised is not None:
        return FAILED, "%s: %s" % (type(raised).__name__, raised)
    if errors:
        return FAILED, "%d ERROR record(s) logged during the step, first: %s" % (
            len(errors), errors[0])
    if skip_reason is not None:
        return SKIPPED, skip_reason
    return OK, None


class ErrorCounter(logging.Handler):
    """A logging handler that keeps the message of every ERROR or worse record.

    Attach it to the logger a step logs to; `messages` then holds what was
    logged at ERROR and CRITICAL while it was attached. A record whose own
    format arguments are wrong is still kept, as its raw format string.
    """

    def __init__(self, level=logging.ERROR):
        super().__init__(level)
        self.messages = []

    def emit(self, record):
        try:
            message = record.getMessage()
        except Exception:  # bad %-args: still an error, still counted
            message = str(record.msg)
        self.messages.append(message)


class StepRun:
    """Handed to the body of ``Checkpoints.step``. Call skip(reason) to skip."""

    def __init__(self):
        self.skip_reason = None
        self.data = {}

    def skip(self, reason):
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("skip() needs a reason; a skip with no reason reads as a success")
        self.skip_reason = reason


class Checkpoints:
    """Record what each named step did, so a re-run resumes instead of redoing.

    Wraps json. State is a small dict written atomically (temp file +
    os.replace) so a crash mid-write cannot leave a half-file. A file that is
    already corrupt or unreadable is reported and treated as "nothing done
    yet". A resume aid must never be the thing that stops the job.

    Each step is recorded as ok, skipped or failed, with the time, a reason,
    and optionally a fingerprint of its input files. Only an ok step is done:
    a resume runs a skipped or failed step again.

    `run_key` (a string, such as the date a daily job is for) scopes the file to
    one run. A file written under a different key is ignored and the run starts
    fresh; the same key resumes. With no run_key the file is always resumed.

    Honest note: unlike the other helpers in this module, this pattern had
    exactly ONE prior use in the corpus that motivated jobharness. It is
    generalized from a single site, not from a repeated one.
    """

    def __init__(self, path, logger=None, run_key=None):
        if run_key is not None and not isinstance(run_key, str):
            raise ValueError("run_key must be a string, such as '2026-10-09'")
        self.path = path
        self.run_key = run_key
        self._log = logger or logging.getLogger(__name__)
        self.state = self._load()

    def _blank(self):
        return {"completed_steps": [], "last_run": None, "data": {},
                "run_key": self.run_key, "steps": {}}

    def _load(self):
        blank = self._blank()
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
        except FileNotFoundError:
            return blank
        except (ValueError, OSError, UnicodeDecodeError) as exc:
            self._log.warning("checkpoint file %s unreadable (%s); starting fresh", self.path, exc)
            return blank
        if (not isinstance(loaded, dict)
                or not isinstance(loaded.get("completed_steps"), list)
                or not isinstance(loaded.get("steps", {}), dict)):
            self._log.warning("checkpoint file %s has unexpected shape; starting fresh", self.path)
            return blank
        if self.run_key is not None and loaded.get("run_key") != self.run_key:
            self._log.info("checkpoint file %s belongs to run %r, not %r; starting fresh",
                           self.path, loaded.get("run_key"), self.run_key)
            return blank
        blank.update(loaded)
        return blank

    def save(self):
        parent = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(parent, exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(self.state, handle, indent=2)
        os.replace(tmp, self.path)

    def is_done(self, step, inputs=None):
        """True only for an ok step. With `inputs`, also only if they are unchanged."""
        if step not in self.state["completed_steps"]:
            return False
        if inputs is None:
            return True
        recorded = (self.state["steps"].get(step) or {}).get("inputs")
        try:
            current = fingerprint(inputs)
        except OSError as exc:
            self._log.info("step %s: an input cannot be read (%s), so it runs again", step, exc)
            return False
        if recorded != current:
            self._log.info("step %s: its inputs changed since it ran, so it runs again", step)
            return False
        return True

    def status(self, step):
        """'ok', 'skipped', 'failed', or None for a step never recorded."""
        record = self.state["steps"].get(step)
        if isinstance(record, dict) and record.get("state") in STATES:
            return record["state"]
        return OK if step in self.state["completed_steps"] else None

    def reason(self, step):
        """Why a step was skipped or failed, or None."""
        record = self.state["steps"].get(step)
        return record.get("reason") if isinstance(record, dict) else None

    def _write(self, step, state, reason, prints, data):
        stamp = _dt.datetime.now().isoformat(timespec="seconds")
        done = self.state["completed_steps"]
        if state == OK:
            if step not in done:
                done.append(step)
        elif step in done:
            done.remove(step)
        if data:
            self.state["data"][step] = data
        self.state["steps"][step] = {"state": state, "at": stamp,
                                     "inputs": prints, "reason": reason}
        self.state["last_run"] = stamp
        self.save()

    def mark_done(self, step, **data):
        self._write(step, OK, None, None, data)

    def record(self, step, state, reason=None, inputs=None, **data):
        """Record `step` as ok, skipped or failed. Skipped and failed need a reason."""
        if state not in STATES:
            raise ValueError("state must be one of %s, not %r" % (", ".join(STATES), state))
        if state != OK and not reason:
            raise ValueError("a %s step needs a reason" % state)
        prints = fingerprint(inputs) if inputs is not None else None
        self._write(step, state, reason, prints, data)

    @contextlib.contextmanager
    def step(self, name, logger=None, inputs=None):
        """Run one step's body and record what it did.

        Inputs are fingerprinted BEFORE the body runs, so a file changed while
        the step reads it does not get recorded as the version the step used.
        While the body runs an ErrorCounter watches `logger` (default: this
        object's logger). The step is recorded failed if the body raises (the
        exception still propagates) or logs ERROR, skipped if it called
        run.skip(reason), and ok otherwise. run.data is saved with the step.
        """
        watched = logger or self._log
        watch = ErrorCounter()
        run = StepRun()
        prints = None
        raised = None
        watched.addHandler(watch)
        try:
            if inputs is not None:
                prints = fingerprint(inputs)
            yield run
        except BaseException as exc:
            raised = exc
            raise
        finally:
            watched.removeHandler(watch)
            state, why = step_outcome(raised, run.skip_reason, watch.messages)
            self._write(name, state, why, prints, run.data)

    def get(self, step, default=None):
        return self.state["data"].get(step, default)

    def clear(self):
        """Forget all recorded steps and remove the state file."""
        self.state = self._blank()
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------------------
# 6. FTPSession  (adopted, not invented. See docstring)
# ---------------------------------------------------------------------------

class FTPSession(ftplib.FTP_TLS):
    """FTP_TLS that reuses the control TLS session and ignores the PASV address.

    ADOPTED, NOT ORIGINAL. Both overrides below are the widely-copied public
    workaround circulated on Stack Overflow and the CPython issue tracker for
    two long-standing interop problems:

      * ntransfercmd: servers configured with "TLS session resumption
        required" (vsftpd's require_ssl_reuse, IIS FTP) reject a data channel
        whose TLS session differs from the control channel's. The fix passes
        ``session=self.sock.session`` when wrapping the data socket. See
        CPython bpo-19500 / gh-63699.
      * makepasv: a server behind NAT answers PASV with its own private
        address. Returning ``self.host`` instead makes the client connect back
        to the host it already reached.

    Reproduced here only so scripts stop pasting it. Credit to the original
    authors of the workaround.
    """

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(
                conn, server_hostname=self.host, session=self.sock.session
            )
        return conn, size

    def makepasv(self):
        _host, port = super().makepasv()
        return self.host, port


# ---------------------------------------------------------------------------
# 7. run_summary
# ---------------------------------------------------------------------------

def run_summary(name, started, finished=None, steps=None, errors=None, extra=None, width=68):
    """Format an end-of-run block for the tail of a scheduled job's log.

    Pure string formatting. Writes nothing, logs nothing. `started` and
    `finished` are datetimes; `finished` defaults to now.
    """
    finished = finished or _dt.datetime.now()
    errors = list(errors or [])
    rule = "=" * width
    elapsed = _dt.timedelta(seconds=int((finished - started).total_seconds()))
    result = "FAILED (%d error(s))" % len(errors) if errors else "OK"
    lines = [rule, "RUN SUMMARY: %s" % (name,), rule,
             "Started  : %s" % (started.strftime(LOG_DATEFMT),),
             "Finished : %s" % (finished.strftime(LOG_DATEFMT),),
             "Elapsed  : %s" % (elapsed,),
             "Result   : %s" % (result,)]
    for key, value in (extra or {}).items():
        lines.append("%-9s: %s" % (str(key)[:9], value))
    if steps:
        lines.append("-" * width)
        for step in steps:
            lines.append("  step: %s" % (step,))
    if errors:
        lines.append("-" * width)
        for err in errors:
            lines.append("  ERROR: %s" % (err,))
    lines.append(rule)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 8. RunLock: one run at a time, and a dead run's lock is refused loudly
# ---------------------------------------------------------------------------

FREE = "free"
HELD = "held"
STALE = "stale"
EXIT_CODES = {FREE: 0, HELD: 3, STALE: 4}

# A lock file that exists but holds no readable record is younger than this
# many seconds only while the run that created it is still writing it.
_WRITE_GRACE = 10.0

_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_ERROR_ACCESS_DENIED = 5
_STILL_ACTIVE = 259


class LockError(RuntimeError):
    """The run lock is held by a live run (exit_code 3) or left by a dead one (4)."""

    def __init__(self, path, verdict, reason):
        self.path = path
        self.verdict = verdict
        self.reason = reason
        self.exit_code = EXIT_CODES[verdict]
        hint = ""
        if verdict == STALE:
            hint = ("; check it with `python jobharness.py --lock %s`, and remove it"
                    " with --apply" % (path,))
        super().__init__("run lock %s is %s: %s%s" % (path, verdict.upper(), reason, hint))


def parse_lock(text):
    """Return the lock record as a dict, or None if it is not a valid record. Pure."""
    try:
        info = json.loads(text)
    except ValueError:
        return None
    if not isinstance(info, dict):
        return None
    pid, ts = info.get("pid"), info.get("ts")
    if isinstance(pid, bool) or not isinstance(pid, int) or not 0 < pid < 2 ** 31:
        return None
    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
        return None
    for key in ("host", "started", "token"):
        if not isinstance(info.get(key), str) or not info[key]:
            return None
    return info


def _age_text(seconds):
    return str(_dt.timedelta(seconds=max(0, int(seconds))))


def lock_verdict(info, now_ts, this_host, max_age, alive, mtime):
    """Decide (verdict, reason) for an existing lock file. Pure: no I/O.

    info     - parse_lock() of the file, or None if it holds no valid record
    now_ts   - epoch seconds now
    this_host- this machine's host name (compared case-insensitively)
    max_age  - seconds; a lock older than this is stale even if its pid lives
    alive    - alive(pid) -> bool, consulted only for a lock from this host
    mtime    - the lock file's mtime, used only when info is None

    A lock is never called stale by guessing: on this host its pid is checked;
    from another host only age can decide, so without max_age it stays held.
    An age exactly equal to max_age is still held.
    """
    if info is None:
        if now_ts - mtime < _WRITE_GRACE:
            return HELD, ("the lock file holds no readable record and is %s old, so a"
                          " starting run may still be writing it" % _age_text(now_ts - mtime))
        return STALE, "the lock file holds no readable record, so no run can be shown to own it"
    age = now_ts - info["ts"]
    owner = "pid %d on %s, started %s, %s ago" % (
        info["pid"], info["host"], info["started"], _age_text(age))
    same_host = info["host"].lower() == this_host.lower()
    if same_host and not alive(info["pid"]):
        return STALE, "%s: that process is gone" % owner
    if max_age is not None and age > max_age:
        return STALE, ("%s: older than the %ds max age, so the run is hung or its pid was"
                       " reused" % (owner, max_age))
    if same_host:
        return HELD, "%s: that process is still running" % owner
    if max_age is None:
        return HELD, ("%s: a pid on another host cannot be checked from here; pass a"
                      " max age to judge it by age" % owner)
    return HELD, "%s: within the %ds max age" % (owner, max_age)


def _pid_alive_posix(pid, kill=os.kill):
    """kill(pid, 0) sends nothing; it only checks that the pid exists."""
    try:
        kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # it exists, it is just not ours to signal
        return True
    return True


class _Kernel32:
    """The three kernel32 calls _pid_alive_windows needs, behind a fakeable face."""

    def __init__(self):
        import ctypes
        from ctypes import wintypes
        self._ctypes = ctypes
        self._dword = wintypes.DWORD
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        k32.OpenProcess.restype = wintypes.HANDLE
        k32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        k32.GetExitCodeProcess.restype = wintypes.BOOL
        k32.CloseHandle.argtypes = (wintypes.HANDLE,)
        k32.CloseHandle.restype = wintypes.BOOL
        self._k32 = k32

    def open(self, pid):
        handle = self._k32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        return handle, self._ctypes.get_last_error()

    def exit_code(self, handle):
        code = self._dword()
        succeeded = self._k32.GetExitCodeProcess(handle, self._ctypes.byref(code))
        return code.value if succeeded else None

    def close(self, handle):
        self._k32.CloseHandle(handle)


def _pid_alive_windows(pid, kernel=None):
    """OpenProcess, then GetExitCodeProcess.

    os.kill(pid, 0) is not a probe on Windows: signal 0 is CTRL_C_EVENT there,
    and any other value terminates the process. A pid whose process has exited
    can still be opened while some handle to it is open, so a successful open
    alone does not mean alive; the exit code must still be STILL_ACTIVE.
    """
    kernel = kernel or _Kernel32()
    handle, error = kernel.open(pid)
    if not handle:
        return error == _ERROR_ACCESS_DENIED  # exists, but not ours to open
    try:
        code = kernel.exit_code(handle)
    finally:
        kernel.close(handle)
    return code is None or code == _STILL_ACTIVE


def pid_alive(pid):
    """True if a process with this pid exists on this host."""
    return (_pid_alive_windows if os.name == "nt" else _pid_alive_posix)(pid)


def inspect_lock(path, max_age=None, now=None, alive=None, host=None):
    """Read a lock file and return (verdict, reason, raw_text). Reads only.

    verdict is FREE (no file), HELD or STALE. A file that exists but cannot be
    read is HELD: nothing proves its owner is gone.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            raw = handle.read()
        mtime = os.path.getmtime(path)
    except FileNotFoundError:
        return FREE, "no lock file at %s" % (path,), None
    except OSError as exc:
        return HELD, "the lock file cannot be read (%s), so it is treated as held" % (exc,), None
    now_ts = time.time() if now is None else now
    verdict, reason = lock_verdict(parse_lock(raw), now_ts, host or socket.gethostname(),
                                   max_age, alive or pid_alive, mtime)
    return verdict, reason, raw


class RunLock:
    """Allow one run of a job at a time, and refuse a dead run's lock loudly.

    acquire() creates `path` with O_CREAT | O_EXCL, so of two runs racing for
    it exactly one succeeds, and writes the pid, host, start time and a random
    token into it. If the file already exists, acquire() raises LockError:
    exit_code 3 when a live run holds it, 4 when the run that wrote it is gone.

    acquire() never deletes a lock, stale or not. Two runs that both judged a
    lock stale and both deleted it would both run. Removing a stale lock is a
    separate, deliberate step: `python jobharness.py --lock PATH --apply`.

    release() deletes the file only if it still holds this run's token.

        with RunLock("nightly.lock", max_age=6 * 3600):
            ...
    """

    def __init__(self, path, max_age=None, logger=None):
        if max_age is not None and max_age < 0:
            raise ValueError("max_age must be >= 0 seconds (or None)")
        self.path = path
        self.max_age = max_age
        self._log = logger or logging.getLogger(__name__)
        self._token = None

    def acquire(self):
        token = os.urandom(8).hex()
        record = json.dumps({"pid": os.getpid(), "host": socket.gethostname(),
                             "started": _dt.datetime.now().isoformat(timespec="seconds"),
                             "ts": time.time(), "token": token})
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            verdict, reason, _raw = inspect_lock(self.path, self.max_age)
            if verdict == FREE:  # released between our open and our read
                verdict, reason = HELD, "the lock was released while it was read; try again"
            raise LockError(self.path, verdict, reason)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(record)
        self._token = token
        return self

    def release(self):
        if self._token is None:
            return
        token, self._token = self._token, None
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                info = parse_lock(handle.read())
        except FileNotFoundError:
            self._log.warning("run lock %s was already gone at release", self.path)
            return
        if info is None or info["token"] != token:
            self._log.warning("run lock %s now belongs to another run; left in place", self.path)
            return
        os.remove(self.path)

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False


# ---------------------------------------------------------------------------
# Command line: --self-test, and a read-only lock check with --apply to remove
# ---------------------------------------------------------------------------

def _parse(argv):
    ap = argparse.ArgumentParser(
        prog="jobharness.py",
        allow_abbrev=False,
        description="Offline self-test, and a check of a RunLock file.",
        epilog="Exit codes for --lock: 0 no lock (or a stale one removed), 3 held, "
               "4 stale. 2 is a usage error. Only --apply removes anything.",
    )
    ap.add_argument("--self-test", dest="self_test", action="store_true",
                    help="run the offline assertions and exit")
    ap.add_argument("--lock", metavar="PATH",
                    help="report whether this run lock is free, held or stale. Reads only.")
    ap.add_argument("--max-age", dest="max_age", type=int, metavar="SECONDS",
                    help="a lock older than this is stale even if its pid is alive. "
                         "Needed to judge a lock written on another host.")
    ap.add_argument("--apply", action="store_true",
                    help="with --lock: remove the lock if, and only if, it is stale")
    args = ap.parse_args(argv)
    if args.max_age is not None and args.max_age < 0:
        ap.error("--max-age cannot be negative")
    if (args.apply or args.max_age is not None) and not args.lock:
        ap.error("--apply and --max-age need --lock")
    return args


def _run_lock(path, max_age, apply):
    verdict, reason, raw = inspect_lock(path, max_age)
    print("%s: %s" % (verdict.upper(), reason))
    if verdict == STALE:
        if not apply:
            print("Nothing was removed. Re-run with --apply to remove this stale lock.")
            return EXIT_CODES[STALE]
        again, _reason, raw_again = inspect_lock(path, max_age)
        if again != STALE or raw_again != raw:
            print("The lock changed while it was checked. Nothing was removed.")
            return EXIT_CODES[again]
        os.remove(path)
        print("Removed %s." % (path,))
        return 0
    if apply and verdict == HELD:
        print("Nothing was removed: a held lock is never removed.")
    return EXIT_CODES[verdict]


def main(argv=None):
    args = _parse(sys.argv[1:] if argv is None else argv)
    if args.self_test:
        return _self_test()
    if args.lock:
        return _run_lock(args.lock, args.max_age, args.apply)
    print(__doc__.strip())
    print("\njobharness %s, run with --self-test" % (__version__,))
    return 0


# ---------------------------------------------------------------------------
# Offline self-test. No network, no credentials, no real clock dependence.
# ---------------------------------------------------------------------------

def _touch(path, mtime=None, body="x"):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(body)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def _make_zip(path, members):
    with zipfile.ZipFile(path, "w") as archive:
        for name, body in members:
            archive.writestr(name, body)
    return path


class _RecordingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(self.format(record))


class _FakeSMTP:
    """Stand-in for smtplib.SMTP. Never opens a socket."""
    instances = []

    def __init__(self, host, port, timeout=None):
        self.host, self.port, self.timeout = host, port, timeout
        self.ehlo_count = 0
        self.tls = False
        self.logins = []
        self.sent = []
        self.quit_count = 0
        self.fail_next_send = False
        self.fail_login = False
        self.fail_quit = False
        _FakeSMTP.instances.append(self)

    def ehlo(self):
        self.ehlo_count += 1

    def starttls(self):
        self.tls = True

    def login(self, user, password):
        if self.fail_login:
            raise smtplib.SMTPAuthenticationError(535, b"simulated auth failure")
        self.logins.append((user, password))

    def sendmail(self, from_addr, to_addrs, msg):
        if self.fail_next_send:
            self.fail_next_send = False
            raise smtplib.SMTPServerDisconnected("simulated idle timeout")
        self.sent.append((from_addr, to_addrs, msg))

    def quit(self):
        self.quit_count += 1
        if self.fail_quit:
            raise smtplib.SMTPServerDisconnected("already gone")


class _FakeKernel:
    """Stand-in for _Kernel32: scripted OpenProcess / GetExitCodeProcess answers."""

    def __init__(self, handle, error=0, code=None):
        self.handle, self.error, self.code = handle, error, code
        self.closed = []

    def open(self, pid):
        return self.handle, self.error

    def exit_code(self, handle):
        return self.code

    def close(self, handle):
        self.closed.append(handle)


class _FakeTLSContext:
    def __init__(self):
        self.calls = []

    def wrap_socket(self, conn, **kwargs):
        self.calls.append(kwargs)
        return ("wrapped", conn)


def _lock_text(pid, host, ts, token="t0k3n", started="2026-01-01T00:00:00"):
    return json.dumps({"pid": pid, "host": host, "started": started, "ts": ts,
                       "token": token})


def _self_test():
    import io
    import subprocess
    import tempfile
    import types
    import warnings

    passed = [0]
    failed = []

    def check(cond, label):
        if cond:
            passed[0] += 1
            print("PASS  %s" % label)
        else:
            failed.append(label)
            print("FAIL  %s" % label)

    def raises(fn, label, exc=ValueError):
        try:
            fn()
        except exc:
            check(True, label)
            return
        except Exception as other:
            check(False, "%s (wrong exception %r)" % (label, other))
            return
        check(False, "%s (no error raised)" % label)

    def caught(fn, exc=Exception):
        """The exception fn raised, or None."""
        try:
            fn()
        except exc as error:
            return error
        return None

    def report():
        total = passed[0] + len(failed)
        print("-" * 68)
        if failed:
            print("%d assertions, %d failed" % (total, len(failed)))
            for label in failed:
                print("  FAILED: %s" % label)
            return 1
        print("%d assertions, 0 failed" % total)
        return 0

    def cli(argv):
        """main(argv) with its output captured: (exit code, stdout text)."""
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            try:
                code = main(argv)
            except SystemExit as stop:
                code = stop.code
        return code, out.getvalue()

    def quiet_logger(name):
        capture = _RecordingHandler()
        capture.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        log = logging.getLogger(name)
        log.handlers = [capture]
        log.propagate = False
        log.setLevel(logging.DEBUG)
        return log, capture

    print("jobharness self-test: no network, no credentials, temp files only")
    print("-" * 68)

    tmp = tempfile.mkdtemp(prefix="jobharness_selftest_")
    try:
        # --- 3. retry ---------------------------------------------------------
        slept = []
        seen = []
        calls = {"n": 0}

        @retry(ValueError, attempts=4, delay=0.5, backoff=2.0,
               on_retry=lambda exc, attempt: seen.append((attempt, str(exc))),
               sleep=slept.append)
        def always_fails():
            calls["n"] += 1
            raise ValueError("boom %d" % calls["n"])

        raised = caught(always_fails, ValueError)
        check(raised is not None, "retry re-raises after exhaustion")
        check(str(raised) == "boom 4", "retry re-raises the LAST exception, not the first")
        check(calls["n"] == 4, "retry made exactly `attempts` calls")
        check(slept == [0.5, 1.0, 2.0], "backoff sequence is delay*backoff**n: got %s" % (slept,))
        check(len(slept) == 3, "no sleep after the final attempt")
        check([a for a, _ in seen] == [1, 2, 3], "on_retry fires with 1-based attempt numbers")
        check(seen[0][1] == "boom 1", "on_retry receives the exception that just failed")

        succeeded = {"n": 0}

        @retry(ValueError, attempts=3, delay=0, sleep=slept.append)
        def fails_once():
            succeeded["n"] += 1
            if succeeded["n"] < 2:
                raise ValueError("transient")
            return "value"

        check(fails_once() == "value", "retry returns the successful result")
        check(succeeded["n"] == 2, "retry stops calling once the function succeeds")

        unlisted = {"n": 0}

        @retry(ValueError, attempts=5, delay=0, sleep=slept.append)
        def wrong_exception():
            unlisted["n"] += 1
            raise KeyError("not in the list")

        check(isinstance(caught(wrong_exception, KeyError), KeyError),
              "an unlisted exception propagates")
        check(unlisted["n"] == 1, "an unlisted exception is NOT retried")

        @retry((ValueError, TypeError), attempts=2, delay=0, sleep=slept.append)
        def named():
            """docstring survives"""
            raise TypeError("t")

        check(named.__name__ == "named", "functools.wraps preserves __name__")
        check(named.__doc__ == "docstring survives", "functools.wraps preserves __doc__")
        raises(lambda: retry(ValueError, attempts=0), "retry with attempts=0 is a ValueError")
        tb_error = caught(named, TypeError)
        frames = []
        tb = tb_error.__traceback__
        while tb is not None:
            frames.append(tb.tb_frame.f_code.co_name)
            tb = tb.tb_next
        check(frames[-1] == "named",
              "the re-raised exception keeps its original traceback down to the failing call")

        # --- 2. safe_extract --------------------------------------------------
        zips = os.path.join(tmp, "zips")
        os.makedirs(zips)

        clean = _make_zip(os.path.join(zips, "clean.zip"),
                          [("a.txt", "alpha"), ("sub/b.txt", "beta")])
        dest = os.path.join(tmp, "out_clean")
        got = safe_extract(clean, dest)
        check(len(got) == 2, "safe_extract returns one path per file member")
        check(all(os.path.isfile(p) for p in got), "returned paths exist on disk")
        check(os.path.abspath(got[0]).startswith(os.path.abspath(dest)),
              "extracted files land inside dest")
        with open(os.path.join(dest, "sub", "b.txt"), encoding="utf-8") as handle:
            check(handle.read() == "beta", "nested member content is intact")

        for label, member in [
            ("../ traversal", "../escape.txt"),
            ("deep ../ traversal", "sub/../../escape.txt"),
            ("backslash traversal", "..\\escape.txt"),
            ("posix absolute path", "/etc/escape.txt"),
            ("drive-letter path", "C:/Windows/escape.txt"),
            ("drive-relative path", "C:escape.txt"),
            ("UNC-ish path", "\\\\server\\share\\escape.txt"),
        ]:
            bad = _make_zip(os.path.join(zips, "bad_%d.zip" % abs(hash(member))),
                            [("ok.txt", "fine"), (member, "pwned")])
            bad_dest = os.path.join(tmp, "out_bad_%d" % abs(hash(member)))
            raises(lambda: safe_extract(bad, bad_dest), "safe_extract refuses %s" % label,
                   UnsafeArchiveError)
            check(not os.path.exists(bad_dest),
                  "refusing %s leaves NOTHING on disk (dest not created)" % label)

        escaped_file = os.path.join(tmp, "escape.txt")
        check(not os.path.exists(escaped_file), "no member ever escaped to the parent directory")

        empty_zip = _make_zip(os.path.join(zips, "empty.zip"), [])
        empty_dest = os.path.join(tmp, "out_empty")
        check(safe_extract(empty_zip, empty_dest) == [], "an empty archive extracts to an empty list")
        check(os.path.isdir(empty_dest), "a clean (even empty) archive creates dest")

        check(_reject_reason("sub/ok.txt") is None, "an ordinary nested name is accepted")
        check(_reject_reason("..") is not None, "a bare '..' member is rejected")
        check(_reject_reason("a..b/ok.txt") is None, "'..' inside a filename is not traversal")

        # The name check and the resolved-path check are two independent layers.
        # Neuter the name check and the containment check must still hold, or
        # deleting the containment block would be a silent no-op.
        real_reject = _reject_reason
        globals()["_reject_reason"] = lambda member_name: None
        try:
            sneak = _make_zip(os.path.join(zips, "nameclean.zip"), [("../escape2.txt", "pwned")])
            sneak_dest = os.path.join(tmp, "out_nameclean")
            sneak_error = caught(lambda: safe_extract(sneak, sneak_dest), UnsafeArchiveError)
            # commonpath raises ValueError for paths on two Windows drives; that
            # must read as "outside", never as a crash or as "contained".
            real_commonpath = os.path.commonpath

            def two_drives(paths):
                raise ValueError("paths are on different drives")
            os.path.commonpath = two_drives
            try:
                drives = _make_zip(os.path.join(zips, "drives.zip"), [("ok.txt", "fine")])
                drive_error = caught(lambda: safe_extract(drives, os.path.join(tmp, "out_drv")),
                                     UnsafeArchiveError)
            finally:
                os.path.commonpath = real_commonpath
        finally:
            globals()["_reject_reason"] = real_reject
        check(sneak_error is not None, "containment refuses even when the NAME check is bypassed")
        check(not os.path.exists(os.path.join(tmp, "escape2.txt")),
              "the bypassed-name member never reached the parent directory")
        check(drive_error is not None and "outside" in str(drive_error),
              "a path on another drive is refused as outside dest, not a crash  <-- pinned defect")

        # A symlinked/junctioned subdirectory of dest: every member name is
        # clean, so only a realpath-based check can catch it. abspath cannot.
        link_dest = os.path.join(tmp, "out_link")
        outside = os.path.join(tmp, "outside_target")
        os.makedirs(link_dest)
        os.makedirs(outside)
        try:
            os.symlink(outside, os.path.join(link_dest, "escape"), target_is_directory=True)
            can_symlink = True
        except (OSError, NotImplementedError, AttributeError):
            can_symlink = False  # unprivileged Windows without Developer Mode
        if can_symlink:
            linked = _make_zip(os.path.join(zips, "linked.zip"), [("escape/pwned.txt", "pwned")])
            raises(lambda: safe_extract(linked, link_dest),
                   "a symlinked subdirectory of dest is refused (realpath, not abspath)",
                   UnsafeArchiveError)
            check(not os.path.exists(os.path.join(outside, "pwned.txt")),
                  "nothing was written through the symlink")
            # ...and the mirror case: a dest that IS a symlink is the caller's own
            # choice of directory and must still extract, not trip containment.
            sym_dest = os.path.join(tmp, "out_symdest")
            os.symlink(os.path.join(tmp, "outside_target"), sym_dest, target_is_directory=True)
            check(len(safe_extract(clean, sym_dest)) == 2,
                  "a dest that is itself a symlink still extracts (no false refusal)")
        else:
            check(True, "symlink containment: SKIPPED, this platform/user cannot create symlinks")
            check(True, "symlink containment: SKIPPED (2 of 3)")
            check(True, "symlink containment: SKIPPED (3 of 3)")

        # Two members writing one path: the second silently overwrites the first
        # under a plain extractall, and the returned list would claim both.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # zipfile warns about the duplicate name
            dup = _make_zip(os.path.join(zips, "dup.zip"),
                            [("config.txt", "real"), ("config.txt", "EVIL")])
        raises(lambda: safe_extract(dup, os.path.join(tmp, "out_dup")),
               "two members resolving to the same path are refused", UnsafeArchiveError)

        cased = _make_zip(os.path.join(zips, "cased.zip"),
                          [("config.txt", "real"), ("CONFIG.TXT", "EVIL")])
        cased_dest = os.path.join(tmp, "out_cased")
        refused = caught(lambda: safe_extract(cased, cased_dest), UnsafeArchiveError) is not None
        collides = os.path.normcase("config.txt") == os.path.normcase("CONFIG.TXT")
        check(refused == collides,
              "a case-only duplicate is refused exactly where the filesystem collides")

        # Containment-clean but unwritable: "sub" as a file, then "sub/x.txt".
        clash = _make_zip(os.path.join(zips, "clash.zip"),
                          [("sub", "i am a file"), ("sub/x.txt", "boom")])
        clash_dest = os.path.join(tmp, "out_clash")
        raises(lambda: safe_extract(clash, clash_dest),
               "a member set that cannot be written raises instead of half-succeeding",
               (UnsafeArchiveError, OSError))
        check(not os.path.exists(clash_dest), "a failed extraction leaves no half-written dest")

        # The same failure into a dest that already existed: what this call wrote
        # goes, the caller's directory and its other files stay.
        kept_dest = os.path.join(tmp, "out_kept")
        os.makedirs(kept_dest)
        mine = _touch(os.path.join(kept_dest, "mine.txt"))
        dir_clash = _make_zip(os.path.join(zips, "dirclash.zip"),
                              [("d/", ""), ("d/f.txt", "ok"), ("d/f.txt/g.txt", "boom")])
        caught(lambda: safe_extract(dir_clash, kept_dest), Exception)
        check(os.path.isfile(mine) and not os.path.exists(os.path.join(kept_dest, "d")),
              "a failed extraction into an existing dest removes only what it wrote")
        dirs = _make_zip(os.path.join(zips, "dirs.zip"), [("d/", ""), ("d/f.txt", "ok")])
        dir_list = safe_extract(dirs, os.path.join(tmp, "out_dirs"))
        check(len(dir_list) == 1 and dir_list[0].endswith("f.txt"),
              "a directory member is created but not listed as an extracted file")
        undo_dest = os.path.join(tmp, "out_undo")
        os.makedirs(os.path.join(undo_dest, "emptydir"))
        _undo_extract([os.path.join(undo_dest, "emptydir"), os.path.join(undo_dest, "gone.txt")],
                      undo_dest, True)
        check(os.path.isdir(undo_dest) and not os.listdir(undo_dest),
              "undo removes a written directory and shrugs off an already-missing file")

        # --- 1. setup_logging -------------------------------------------------
        logs = os.path.join(tmp, "logs")
        logger, path = setup_logging("jobA", logs, console=False)
        check(os.path.isfile(path), "setup_logging creates the log file")
        check(_log_pattern("jobA").match(os.path.basename(path)),
              "log filename is <name>_YYYYmmdd_HHMMSS.log")
        logger.info("hello from the self-test")
        for handler in logger.handlers:
            handler.flush()
        with open(path, encoding="utf-8") as handle:
            body = handle.read()
        check("hello from the self-test" in body, "setup_logging writes records to the file")
        check("INFO" in body, "the file handler uses the module formatter")

        raises(lambda: setup_logging("jobA", logs, retain_days=7, retain_count=5, console=False),
               "passing two retention policies is a ValueError")
        for bad_kwargs in ({"retain_days": 0}, {"retain_count": 0}):
            raises(lambda: setup_logging("jobA", logs, console=False, **bad_kwargs),
                   "a non-positive retention value is rejected: %s" % (bad_kwargs,))
        raises(lambda: setup_logging("", logs, console=False), "an empty job name is rejected")

        foreign = _RecordingHandler()
        logger.addHandler(foreign)
        later = _dt.datetime.now() + _dt.timedelta(seconds=5)
        logger_again, _path_again = setup_logging("jobA", logs, console=True, now=later)
        foreign_kept = foreign in logger_again.handlers
        logger_again.removeHandler(foreign)
        kinds = sorted(type(h).__name__ for h in logger_again.handlers)
        check(kinds == ["FileHandler", "StreamHandler"],
              "a second setup_logging call replaces its handlers instead of stacking them")
        check(foreign_kept and logger_again is logger,
              "a handler setup_logging did not install survives the second call")
        console_handlers = [h for h in logger_again.handlers
                            if type(h) is logging.StreamHandler]
        check(len(console_handlers) == 1 and console_handlers[0].stream is sys.stdout,
              "console=True adds one console handler on stdout")

        # Destructive default OFF.
        off_dir = os.path.join(tmp, "logs_off")
        os.makedirs(off_dir)
        ancient = time.time() - 400 * 86400
        _touch(os.path.join(off_dir, "jobB_20200101_000000.log"), ancient)
        _touch(os.path.join(off_dir, "jobB_20200102_000000.log"), ancient)
        setup_logging("jobB", off_dir, console=False)
        check(len(os.listdir(off_dir)) == 3,
              "no retention argument deletes NOTHING (destructive default is off)")

        # Prune by age.
        age_dir = os.path.join(tmp, "logs_age")
        os.makedirs(age_dir)
        day = 86400
        now_ts = time.time()
        old = _touch(os.path.join(age_dir, "jobC_20200101_000000.log"), now_ts - 10 * day)
        mid = _touch(os.path.join(age_dir, "jobC_20200105_000000.log"), now_ts - 5 * day)
        new = _touch(os.path.join(age_dir, "jobC_20200109_000000.log"), now_ts - 1 * day)
        other_job = _touch(os.path.join(age_dir, "jobD_20200101_000000.log"), now_ts - 99 * day)
        not_a_log = _touch(os.path.join(age_dir, "jobC_notes.log"), now_ts - 99 * day)
        unrelated = _touch(os.path.join(age_dir, "important.txt"), now_ts - 99 * day)
        dir_named_log = os.path.join(age_dir, "jobC_20200101_000001.log")
        os.makedirs(dir_named_log)
        os.utime(dir_named_log, (now_ts - 99 * day, now_ts - 99 * day))
        _log_c, path_c = setup_logging("jobC", age_dir, retain_days=7, console=False)
        check(not os.path.exists(old), "prune-by-age deletes a log older than the cutoff")
        check(os.path.exists(mid), "prune-by-age keeps a log newer than the cutoff")
        check(os.path.exists(new), "prune-by-age keeps the newest log")
        check(os.path.exists(path_c), "prune-by-age never deletes the log it just opened")
        check(os.path.exists(other_job), "pruning never touches ANOTHER job's logs")
        check(os.path.exists(not_a_log), "pruning never touches a non-timestamped .log file")
        check(os.path.exists(unrelated), "pruning never touches an unrelated file")
        check(os.path.isdir(dir_named_log), "pruning never touches a directory named like a log")

        # Age cutoff boundary, pinned exactly: `now` is injected, so mtime ==
        # cutoff is a real case and not a race. Equal is KEPT, strictly older goes.
        bnd_dir = os.path.join(tmp, "logs_boundary")
        os.makedirs(bnd_dir)
        base = float(int(time.time()))
        cutoff = base - 7 * day
        on_cutoff = _touch(os.path.join(bnd_dir, "jobG_20200101_000000.log"), cutoff)
        past_cutoff = _touch(os.path.join(bnd_dir, "jobG_20200102_000000.log"), cutoff - 1)
        _log_g, _path_g = setup_logging("jobG", bnd_dir, retain_days=7, console=False,
                                        now=_dt.datetime.fromtimestamp(base))
        check(os.path.exists(on_cutoff), "prune-by-age KEEPS a log whose mtime is exactly the cutoff")
        check(not os.path.exists(past_cutoff), "prune-by-age deletes one second past the cutoff")

        # Prune by count.
        cnt_dir = os.path.join(tmp, "logs_count")
        os.makedirs(cnt_dir)
        for i in range(1, 6):
            _touch(os.path.join(cnt_dir, "jobE_2020010%d_000000.log" % i),
                   now_ts - (10 - i) * day)
        decoy_job = _touch(os.path.join(cnt_dir, "jobF_20200101_000000.log"), now_ts - 99 * day)
        decoy_name = _touch(os.path.join(cnt_dir, "jobE_backup.log"), now_ts - 99 * day)
        _log_e, path_e = setup_logging("jobE", cnt_dir, retain_count=3, console=False)
        kept = sorted(f for f in os.listdir(cnt_dir) if _log_pattern("jobE").match(f))
        check(len(kept) == 3, "prune-by-count keeps exactly N: got %s" % (kept,))
        check(os.path.basename(path_e) in kept, "the log just opened counts toward N and survives")
        check("jobE_20200101_000000.log" not in kept, "prune-by-count drops the oldest first")
        check("jobE_20200105_000000.log" in kept, "prune-by-count keeps the newest of the old logs")
        check(os.path.exists(decoy_job), "prune-by-count never touches another job's logs")
        check(os.path.exists(decoy_name), "prune-by-count never touches a non-timestamped .log file")

        # Pruning is best effort: a log another process holds, or one that
        # vanishes mid-scan, is reported or skipped, never fatal.
        prune_log, prune_capture = quiet_logger("jobharness.selftest.prune")
        check(_prune_logs(os.path.join(tmp, "no_such_dir"), "jobH", 7, None, "x", prune_log,
                          now_ts) == [],
              "pruning a directory that does not exist removes nothing and does not raise")
        busy_dir = os.path.join(tmp, "logs_busy")
        os.makedirs(busy_dir)
        busy = _touch(os.path.join(busy_dir, "jobH_20200101_000000.log"), now_ts - 99 * day)
        real_remove = os.remove
        real_getmtime = os.path.getmtime

        def refuse_remove(target):
            raise PermissionError("held by another process")
        os.remove = refuse_remove
        try:
            removed = _prune_logs(busy_dir, "jobH", 7, None, "x", prune_log, now_ts)
        finally:
            os.remove = real_remove
        check(removed == [] and os.path.exists(busy),
              "a log that cannot be deleted is kept and pruning carries on")
        check(any(rec.startswith("WARNING could not prune") for rec in prune_capture.records),
              "and the failed delete is logged as a warning")

        def vanish(target):
            raise FileNotFoundError(target)
        os.path.getmtime = vanish
        try:
            vanished = _prune_logs(busy_dir, "jobH", 7, None, "x", prune_log, now_ts)
        finally:
            os.path.getmtime = real_getmtime
        check(vanished == [], "a log that vanishes between listing and stat is skipped")

        for lg in (logger, _log_c, _log_e, _log_g, logging.getLogger("jobB")):
            for handler in list(lg.handlers):
                lg.removeHandler(handler)
                handler.close()

        # --- 5. Checkpoints ---------------------------------------------------
        cp_path = os.path.join(tmp, "state", "checkpoints.json")
        cp = Checkpoints(cp_path)
        check(cp.is_done("download") is False, "a fresh Checkpoints reports nothing done")
        cp.mark_done("download", rows=1234)
        check(os.path.isfile(cp_path), "mark_done writes the state file")
        check(cp.is_done("download"), "mark_done records the step")

        resumed = Checkpoints(cp_path)
        check(resumed.is_done("download"), "a new Checkpoints round-trips completed steps")
        check(resumed.get("download") == {"rows": 1234}, "per-step data round-trips through json")
        check(resumed.is_done("publish") is False, "an unrecorded step is not marked done")

        ran = []
        for step in ("download", "transform", "publish"):
            if resumed.is_done(step):
                continue
            ran.append(step)
            resumed.mark_done(step)
        check(ran == ["transform", "publish"], "resume skips the completed step")
        check(len(resumed.state["completed_steps"]) == 3, "no duplicate step entries")
        resumed.mark_done("publish")
        check(len(resumed.state["completed_steps"]) == 3, "mark_done is idempotent")

        corrupt_path = os.path.join(tmp, "corrupt.json")
        _touch(corrupt_path, body="{not json at all,,,")
        quiet, cp_capture = quiet_logger("jobharness.selftest.corrupt")
        recovered = Checkpoints(corrupt_path, logger=quiet)
        check(recovered.state["completed_steps"] == [], "a corrupt state file recovers as empty")
        check(any("unreadable" in rec for rec in cp_capture.records),
              "a corrupt state file is REPORTED, not silently swallowed")
        check(any(rec.startswith("WARNING") for rec in cp_capture.records),
              "the corrupt-file report is a warning, not a debug line")
        recovered.mark_done("after_corruption")
        check(Checkpoints(corrupt_path, logger=quiet).is_done("after_corruption"),
              "a recovered Checkpoints can still save")

        wrong_shape = os.path.join(tmp, "wrongshape.json")
        _touch(wrong_shape, body='["a", "list", "not", "a", "dict"]')
        cp_capture.records = []
        check(Checkpoints(wrong_shape, logger=quiet).state["completed_steps"] == [],
              "an unexpected JSON shape recovers as empty")
        check(any("unexpected shape" in rec for rec in cp_capture.records),
              "an unexpected JSON shape is REPORTED, not silently swallowed")

        cp.clear()
        check(not os.path.exists(cp_path), "clear() removes the state file")
        check(cp.is_done("download") is False, "clear() forgets completed steps")
        cp.clear()
        check(True, "clear() on a missing file does not raise")

        # --- 5b. step states, run key, fingerprints, ERROR handler ------------
        check(step_outcome(None, None, []) == (OK, None), "a quiet step that returns is ok")
        check(step_outcome(None, "no new file", [])[0] == SKIPPED, "a step that skipped is skipped")
        check(step_outcome(None, None, ["db timeout"])[0] == FAILED,
              "a step that logged an ERROR and returned is FAILED, not ok  <-- pinned defect")
        check(step_outcome(None, "no new file", ["db timeout"])[0] == FAILED,
              "an ERROR beats a skip: a step that errored and then skipped failed")
        boom = RuntimeError("disk full")
        check(step_outcome(boom, "x", ["e"]) == (FAILED, "RuntimeError: disk full"),
              "an exception beats everything and names its type")
        check("1 ERROR record(s)" in step_outcome(None, None, ["db timeout"])[1]
              and "db timeout" in step_outcome(None, None, ["db timeout"])[1],
              "a failed step's reason counts the ERROR records and quotes the first")

        st_dir = os.path.join(tmp, "steps")
        os.makedirs(st_dir)
        st_log, st_capture = quiet_logger("jobharness.selftest.steps")
        in_a = _touch(os.path.join(st_dir, "a.csv"), body="id,x\n1,2\n")
        in_b = _touch(os.path.join(st_dir, "b.csv"), body="id,y\n3,4\n")
        fp_ab = fingerprint([in_a, in_b])
        check(fp_ab.startswith("sha256:") and len(fp_ab) == 7 + 64, "a fingerprint is sha256 hex")
        check(fingerprint([in_b, in_a]) == fp_ab, "the order inputs are listed in does not matter")
        check(fingerprint(in_a) == fingerprint([in_a]), "one path and a list of one agree")
        _touch(in_a, body="id,y\n3,4\n")
        _touch(in_b, body="id,x\n1,2\n")
        check(fingerprint([in_a, in_b]) != fp_ab,
              "swapping the contents of two inputs changes the fingerprint  <-- pinned defect")
        split1 = _touch(os.path.join(st_dir, "s1"), body="ab")
        split2 = _touch(os.path.join(st_dir, "s2"), body="")
        before_split = fingerprint([split1, split2])
        _touch(split1, body="a")
        _touch(split2, body="b")
        check(fingerprint([split1, split2]) != before_split,
              "moving a byte from one input to the next changes the fingerprint  <-- pinned defect")
        raises(lambda: fingerprint(os.path.join(st_dir, "missing.csv")),
               "a missing input raises instead of hashing to something", OSError)

        sp = os.path.join(st_dir, "state.json")
        cps = Checkpoints(sp, logger=st_log, run_key="2026-10-09")
        with cps.step("download", inputs=[in_a]) as run:
            run.data["rows"] = 2
        check(cps.status("download") == OK and cps.is_done("download"),
              "a step body that returns quietly is recorded ok and done")
        check(cps.get("download") == {"rows": 2}, "run.data is saved with the step")
        check(cps.is_done("download", inputs=[in_a]),
              "with unchanged inputs an ok step is still done")
        _touch(in_a, body="id,x\n1,2\n5,6\n")
        check(cps.is_done("download", inputs=[in_a]) is False,
              "a changed input makes an ok step not done, so it runs again  <-- pinned defect")
        check(any("inputs changed" in rec for rec in st_capture.records),
              "and the reason it runs again is logged")
        check(cps.is_done("download", inputs=[os.path.join(st_dir, "nope.csv")]) is False,
              "a missing input makes is_done False, not a crash")

        with cps.step("transform", logger=st_log) as run:
            st_log.error("3 rows failed to geocode")
        check(cps.status("transform") == FAILED,
              "a body that logged ERROR and carried on is recorded failed  <-- pinned defect")
        check(cps.is_done("transform") is False, "a failed step is not done, so a resume runs it")
        check("3 rows failed to geocode" in cps.reason("transform"),
              "the failed step's reason quotes the ERROR it logged")
        check(not any(isinstance(h, ErrorCounter) for h in st_log.handlers),
              "the ERROR watcher is removed when the step ends")

        st_log.error("an error logged before the step")
        child = logging.getLogger("jobharness.selftest.steps.child")
        with cps.step("validate", logger=st_log):
            st_log.warning("a warning is not a failure")
        check(cps.status("validate") == OK,
              "an ERROR logged before the step, and a WARNING during it, are not counted")
        with cps.step("validate2", logger=st_log):
            child.error("from a child logger")
        check(cps.status("validate2") == FAILED, "an ERROR from a child logger is counted")
        bare_log = logging.getLogger("jobharness.selftest.bare")
        bare_log.propagate = False  # only the step's own watcher sees this record
        with cps.step("validate3", logger=bare_log):
            bare_log.error("%d rows", "not a number")
        check(cps.status("validate3") == FAILED,
              "an ERROR whose own format arguments are wrong is still counted  <-- pinned defect")

        with cps.step("publish", logger=st_log) as run:
            run.skip("no new rows since the last run")
        check(cps.status("publish") == SKIPPED, "a body that called skip() is recorded skipped")
        check(cps.is_done("publish") is False,
              "a skipped step is not done, so a resume runs it again  <-- pinned defect")
        check(cps.reason("publish") == "no new rows since the last run",
              "the skip reason is kept")
        raises(lambda: StepRun().skip(""), "skip() with no reason is a ValueError")
        raises(lambda: StepRun().skip(None), "skip(None) is a ValueError")

        def failing_step():
            with cps.step("upload", logger=st_log):
                raise ConnectionError("portal unreachable")
        raises(failing_step, "an exception inside a step still propagates", ConnectionError)
        check(cps.status("upload") == FAILED and "ConnectionError" in cps.reason("upload"),
              "and the step is recorded failed with the exception named")

        def missing_input_step():
            nope = [os.path.join(st_dir, "nope.csv")]
            with cps.step("load", logger=st_log, inputs=nope): ran.append("load body")  # noqa: E701
        raises(missing_input_step, "a missing input fails the step before its body runs",
               OSError)
        check("load body" not in ran and cps.status("load") == FAILED,
              "and the body never ran, and the step is recorded failed")

        cps.mark_done("redo")

        def redo_fails():
            with cps.step("redo", logger=st_log):
                raise RuntimeError("second attempt broke")
        caught(redo_fails)
        check(cps.is_done("redo") is False and "redo" not in cps.state["completed_steps"],
              "a step that was ok and then fails is no longer done  <-- pinned defect")

        # Fingerprint is taken before the body: an input edited DURING the
        # step must not be recorded as the version the step read.
        live_in = _touch(os.path.join(st_dir, "live.csv"), body="v1")
        with cps.step("read_live", logger=st_log, inputs=[live_in]):
            _touch(live_in, body="v2 written while the step ran")
        check(cps.is_done("read_live", inputs=[live_in]) is False,
              "an input changed while the step ran is caught on the next run  <-- pinned defect")

        again = Checkpoints(sp, logger=st_log, run_key="2026-10-09")
        check(again.status("download") == OK and again.status("transform") == FAILED
              and again.status("publish") == SKIPPED,
              "the same run key resumes, and a resume tells ok from skipped from failed")
        check(again.status("never") is None and again.reason("never") is None,
              "a step never recorded has no state and no reason")
        st_capture.records = []
        fresh = Checkpoints(sp, logger=st_log, run_key="2026-10-10")
        check(fresh.status("download") is None and fresh.state["completed_steps"] == [],
              "a different run key starts fresh  <-- pinned defect")
        check(any("belongs to run" in rec for rec in st_capture.records),
              "and says which run the file belonged to")
        check(Checkpoints(sp, logger=st_log).status("download") == OK,
              "no run key resumes whatever the file holds, as before")
        raises(lambda: Checkpoints(sp, run_key=_dt.date(2026, 10, 9)),
               "a run key that is not a string is a ValueError")

        legacy = os.path.join(st_dir, "legacy.json")
        _touch(legacy, body='{"completed_steps": ["download"], "last_run": null, "data": {}}')
        old_cp = Checkpoints(legacy, logger=st_log)
        check(old_cp.is_done("download") and old_cp.status("download") == OK,
              "a checkpoint file written before step states still reads as done  <-- pinned defect")
        check(old_cp.is_done("download", inputs=[in_a]) is False,
              "an old done step with no recorded fingerprint runs again when inputs are given")
        bad_steps = os.path.join(st_dir, "badsteps.json")
        _touch(bad_steps, body='{"completed_steps": [], "steps": ["not", "a", "dict"]}')
        check(Checkpoints(bad_steps, logger=st_log).state["steps"] == {},
              "a steps field of the wrong type recovers as empty")

        rec_cp = Checkpoints(os.path.join(st_dir, "rec.json"), logger=st_log)
        rec_cp.record("fetch", SKIPPED, reason="holiday", inputs=[in_b], note="x")
        check(rec_cp.status("fetch") == SKIPPED and rec_cp.get("fetch") == {"note": "x"},
              "record() stores the state, the reason and the data")
        check(rec_cp.state["steps"]["fetch"]["inputs"] == fingerprint(in_b),
              "record() stores the input fingerprint")
        rec_cp.record("fetch", OK)
        check(rec_cp.is_done("fetch"), "record(OK) marks the step done")
        raises(lambda: rec_cp.record("fetch", "done"), "an unknown state is a ValueError")
        raises(lambda: rec_cp.record("fetch", FAILED), "a failed state with no reason is a ValueError")

        counter = ErrorCounter()
        counter_log = logging.getLogger("jobharness.selftest.counter")
        counter_log.propagate = False
        counter_log.setLevel(logging.DEBUG)
        counter_log.addHandler(counter)
        counter_log.info("info")
        counter_log.warning("warn")
        counter_log.error("err %s", "one")
        counter_log.critical("crit")
        counter_log.removeHandler(counter)
        check(counter.messages == ["err one", "crit"],
              "ErrorCounter keeps ERROR and CRITICAL messages only, formatted")

        # --- 4. EmailNotifier -------------------------------------------------
        _FakeSMTP.instances = []
        fake_env = "JOBHARNESS_SELFTEST_PASSWORD"
        fake_password = "not-a-real-password-0000"
        os.environ[fake_env] = fake_password
        try:
            capture = _RecordingHandler()
            capture.setFormatter(logging.Formatter("%(message)s"))
            mail_log = logging.getLogger("jobharness.selftest.mail")
            mail_log.handlers = [capture]
            mail_log.propagate = False
            mail_log.setLevel(logging.DEBUG)

            notifier = EmailNotifier("smtp.invalid", 25, user="svc-account",
                                     password_env=fake_env, use_tls=True,
                                     smtp_factory=_FakeSMTP, logger=mail_log)
            notifier.send("subject one", "body one", "ops@example.invalid",
                          "jobs@example.invalid")
            check(len(_FakeSMTP.instances) == 1, "a clean send opens exactly one connection")
            first = _FakeSMTP.instances[0]
            check(len(first.sent) == 1, "a clean send calls sendmail exactly once")
            check(first.tls is True, "use_tls issues STARTTLS")
            check(first.logins == [("svc-account", fake_password)],
                  "the password is read from the named environment variable")
            check("subject one" in first.sent[0][2], "the subject reaches the wire")
            check(first.sent[0][1] == ["ops@example.invalid"], "recipients reach the wire")

            first.fail_next_send = True
            notifier.send("subject two", "body two", ["ops@example.invalid"],
                          "jobs@example.invalid")
            check(len(_FakeSMTP.instances) == 2, "SMTPServerDisconnected triggers exactly one reconnect")
            second = _FakeSMTP.instances[1]
            check(len(second.sent) == 1, "the message is re-sent once on the new connection")
            check(first.quit_count == 1, "the dead connection is closed before reconnecting")

            # Third send, nothing wrong with the socket: reconnecting here would
            # be indistinguishable from the reconnect above with only two sends.
            notifier.send("subject three", "body three", "ops@example.invalid",
                          "jobs@example.invalid")
            check(len(_FakeSMTP.instances) == 2,
                  "a later clean send REUSES the connection instead of reconnecting")
            check(len(second.sent) == 2, "the third message went out on the existing connection")

            check(fake_password not in repr(notifier), "the password is not in repr()")
            check(fake_password not in str(vars(notifier)), "the password is not stored on the instance")
            check(all(fake_password not in rec for rec in capture.records),
                  "the password never reaches a log record")
            check(any("reconnect" in rec.lower() for rec in capture.records),
                  "the reconnect is logged")
            check(notifier.password_env == fake_env, "only the env var NAME is retained")

            raises(lambda: notifier.send("s", "b", [], "jobs@example.invalid"),
                   "sending with no recipients is a ValueError")

            # A login that raises must not leave the plaintext bound in our own
            # frame, where the caller's traceback would carry it to a debug page
            # or an error reporter. (smtplib's login frame is not ours to scrub.)
            def _failing_factory(host, port, timeout=None):
                smtp = _FakeSMTP(host, port, timeout=timeout)
                smtp.fail_login = True
                return smtp

            bad_login = EmailNotifier("smtp.invalid", 25, user="svc-account",
                                      password_env=fake_env, smtp_factory=_failing_factory,
                                      logger=mail_log)
            login_error = caught(bad_login.connect, smtplib.SMTPAuthenticationError)
            frames = []
            tb = login_error.__traceback__ if login_error is not None else None
            while tb is not None:
                frames.append(tb.tb_frame)
                tb = tb.tb_next
            check(login_error is not None, "a failed login propagates")
            ours = [f for f in frames if f.f_code.co_name == "connect"]
            check(len(ours) == 1, "the traceback contains EmailNotifier.connect's frame")
            check(all(fake_password not in str(v) for f in ours for v in f.f_locals.values()),
                  "a failed login leaves no password in connect()'s frame locals")

            del os.environ[fake_env]
            missing = EmailNotifier("smtp.invalid", 25, user="svc-account",
                                    password_env=fake_env, smtp_factory=_FakeSMTP,
                                    logger=mail_log)
            raises(missing.connect, "an unset password env var fails loudly, not silently",
                   RuntimeError)

            _FakeSMTP.instances = []
            with EmailNotifier("smtp.invalid", 25, smtp_factory=_FakeSMTP,
                               logger=mail_log) as anon:
                anon.send("s", "b", "ops@example.invalid", "jobs@example.invalid")
                anon._smtp.fail_quit = True
            anon_smtp = _FakeSMTP.instances[0]
            check(anon_smtp.logins == [] and len(anon_smtp.sent) == 1,
                  "with no user the notifier sends without a login")
            check(anon._smtp is None and anon_smtp.quit_count == 1,
                  "leaving the with block closes the connection, even when quit() raises")
            anon.close()
            check(anon._smtp is None, "close() on a closed notifier is a no-op")
        finally:
            os.environ.pop(fake_env, None)

        # --- 6. FTPSession ----------------------------------------------------
        check("ntransfercmd" in FTPSession.__dict__, "FTPSession overrides ntransfercmd")
        check("makepasv" in FTPSession.__dict__, "FTPSession overrides makepasv")
        check(issubclass(FTPSession, ftplib.FTP_TLS), "FTPSession subclasses ftplib.FTP_TLS")
        original = ftplib.FTP_TLS.makepasv
        try:
            ftplib.FTP_TLS.makepasv = lambda self: ("10.0.0.7", 50123)  # private NAT address
            session = FTPSession()  # no host -> ftplib does not connect
            check(session.sock is None, "constructing FTPSession opens no socket")
            session.host = "files.example.invalid"
            check(session.makepasv() == ("files.example.invalid", 50123),
                  "makepasv returns the ORIGINAL host and the server's port")
        finally:
            ftplib.FTP_TLS.makepasv = original

        # ntransfercmd against a fake data socket and TLS context: the override
        # must hand the control channel's session to the data channel, and the
        # stock method must not, or the override has no reason to exist.
        original_ntransfer = ftplib.FTP.ntransfercmd
        try:
            ftplib.FTP.ntransfercmd = lambda self, cmd, rest=None: ("rawconn", 42)
            session = FTPSession()
            session.host = "files.example.invalid"
            session.context = _FakeTLSContext()
            session.sock = types.SimpleNamespace(session="control-session")
            session._prot_p = True
            conn, size = session.ntransfercmd("RETR a.zip")
            check(conn == ("wrapped", "rawconn") and size == 42,
                  "ntransfercmd wraps the data socket when PROT P is on")
            check(session.context.calls[-1].get("session") == "control-session"
                  and session.context.calls[-1].get("server_hostname") == "files.example.invalid",
                  "the data channel reuses the control channel's TLS session")
            ftplib.FTP_TLS.ntransfercmd(session, "RETR a.zip")
            check("session" not in session.context.calls[-1],
                  "stock FTP_TLS.ntransfercmd passes no session, which is the defect fixed here")
            session._prot_p = False
            check(session.ntransfercmd("LIST") == ("rawconn", 42),
                  "with PROT C the data socket is returned unwrapped")
            session.sock = None
        finally:
            ftplib.FTP.ntransfercmd = original_ntransfer

        # --- 7. run_summary ---------------------------------------------------
        start = _dt.datetime(2024, 3, 1, 8, 0, 0)
        end = _dt.datetime(2024, 3, 1, 8, 42, 30)
        clean_text = run_summary("nightly", start, end, steps=["download", "publish"])
        check("Elapsed  : 0:42:30" in clean_text, "run_summary computes elapsed time")
        check("Result   : OK" in clean_text, "run_summary reports OK with no errors")
        check("step: download" in clean_text, "run_summary lists steps")
        failed_text = run_summary("nightly", start, end, errors=["disk full", "timeout"])
        check("FAILED (2 error(s))" in failed_text, "run_summary counts errors")
        check("ERROR: disk full" in failed_text, "run_summary lists errors")
        extra_text = run_summary("nightly", start, end, extra={"rows_written": (1, 2)})
        check("rows_writ: (1, 2)" in extra_text,
              "run_summary prints extra keys cut to 9 characters, tuples intact")

        # --- 8. RunLock -------------------------------------------------------
        here = socket.gethostname()
        valid = _lock_text(4242, "host-a", 1000.0)
        check(parse_lock(valid)["pid"] == 4242, "a well-formed lock record parses")
        for label, text in [
            ("text that is not json", "{not json"),
            ("json that is not an object", "[1, 2]"),
            ("a pid that is true", _lock_text(True, "h", 1.0)),
            ("a pid of 0", _lock_text(0, "h", 1.0)),
            ("a negative pid", _lock_text(-5, "h", 1.0)),
            ("a pid too large for a pid", _lock_text(2 ** 31, "h", 1.0)),
            ("a time that is a string", _lock_text(1, "h", "noon")),
            ("a time that is true", _lock_text(1, "h", True)),
            ("an empty host", _lock_text(1, "", 1.0)),
            ("a missing token", json.dumps({"pid": 1, "host": "h", "ts": 1.0,
                                            "started": "s"})),
        ]:
            check(parse_lock(text) is None, "a lock with %s is not a record" % label)

        alive_fn = lambda pid: True  # noqa: E731
        dead_fn = lambda pid: False  # noqa: E731
        info = parse_lock(_lock_text(4242, "HOST-A", 1000.0))
        check(lock_verdict(info, 1100.0, "host-a", None, alive_fn, 0)[0] == HELD,
              "a lock whose pid runs on this host is held")
        check(lock_verdict(info, 1100.0, "host-a", None, dead_fn, 0)[0] == STALE,
              "a lock whose pid is gone from this host is stale  <-- pinned defect")
        check(lock_verdict(info, 1100.0, "host-a", 99, alive_fn, 0)[0] == STALE,
              "a live pid older than the max age is stale: hung, or a reused pid")
        check(lock_verdict(info, 1100.0, "host-a", 100, alive_fn, 0)[0] == HELD,
              "an age exactly equal to the max age is still held")
        check(lock_verdict(info, 1100.0, "host-b", None, dead_fn, 0)[0] == HELD,
              "another host's lock with no max age is held: its pid cannot be checked here")
        check(lock_verdict(info, 1100.0, "host-b", 50, dead_fn, 0)[0] == STALE,
              "another host's lock older than the max age is stale")
        check(lock_verdict(info, 1100.0, "host-b", 500, dead_fn, 0)[0] == HELD,
              "another host's lock within the max age is held")
        probed = []
        lock_verdict(info, 1100.0, "host-b", 500, lambda pid: probed.append(pid), 0)
        check(probed == [], "a pid from another host is never probed on this one")
        check(lock_verdict(None, 1005.0, here, None, alive_fn, 1000.0)[0] == HELD,
              "an unreadable lock a few seconds old is held: its run may be writing it")
        check(lock_verdict(None, 2000.0, here, None, alive_fn, 1000.0)[0] == STALE,
              "an unreadable lock older than that is stale")
        check("pid 4242 on HOST-A" in lock_verdict(info, 1100.0, "host-a", None, dead_fn, 0)[1],
              "the verdict names the pid and the host")

        check(_pid_alive_posix(7, kill=lambda pid, sig: None) is True,
              "posix: a pid that accepts signal 0 is alive")

        def no_such(pid, sig):
            raise ProcessLookupError(pid)

        def not_ours(pid, sig):
            raise PermissionError(pid)
        check(_pid_alive_posix(7, kill=no_such) is False, "posix: ESRCH means gone")
        check(_pid_alive_posix(7, kill=not_ours) is True,
              "posix: EPERM means alive, just not ours  <-- pinned defect")
        k_denied = _FakeKernel(None, error=_ERROR_ACCESS_DENIED)
        check(_pid_alive_windows(7, k_denied) is True,
              "windows: OpenProcess access denied means alive, just not ours")
        check(_pid_alive_windows(7, _FakeKernel(None, error=87)) is False,
              "windows: OpenProcess invalid parameter means gone")
        k_running = _FakeKernel(1234, code=_STILL_ACTIVE)
        check(_pid_alive_windows(7, k_running) is True and k_running.closed == [1234],
              "windows: exit code STILL_ACTIVE means alive, and the handle is closed")
        k_exited = _FakeKernel(1234, code=0)
        check(_pid_alive_windows(7, k_exited) is False,
              "windows: a process that opens but has an exit code is gone  <-- pinned defect")
        check(_pid_alive_windows(7, _FakeKernel(1234, code=None)) is True,
              "windows: an exit code that cannot be read is treated as alive")
        check(pid_alive(os.getpid()) is True, "this process is alive by the real probe")
        finished = subprocess.Popen([sys.executable, "-c", "pass"])
        finished.wait()
        # On Windows the Popen object still holds a handle to the exited
        # process, so this exercises the exit-code path, not just OpenProcess.
        check(pid_alive(finished.pid) is False, "a child that has exited is gone by the real probe")

        lock_dir = os.path.join(tmp, "locks")
        lock_path = os.path.join(lock_dir, "nightly.lock")
        lock = RunLock(lock_path)
        check(lock.acquire() is lock and os.path.isfile(lock_path),
              "acquire() creates the lock file")
        with open(lock_path, encoding="utf-8") as handle:
            written = parse_lock(handle.read())
        check(written is not None and written["pid"] == os.getpid() and written["host"] == here,
              "the lock records this pid and this host")
        second_try = caught(RunLock(lock_path).acquire, LockError)
        check(second_try is not None and second_try.verdict == HELD
              and second_try.exit_code == 3,
              "a second run while the first is live gets LockError, exit code 3")
        lock.release()
        check(not os.path.exists(lock_path), "release() removes the lock")
        lock.release()
        check(True, "release() twice does not raise")

        def crash_inside():
            with RunLock(lock_path):
                raise KeyError("job blew up")
        raises(crash_inside, "an exception inside the lock still propagates", KeyError)
        check(not os.path.exists(lock_path), "and the lock is released on the way out")

        # A real second process: held while it runs, stale once it is gone.
        sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            _touch(lock_path, body=_lock_text(sleeper.pid, here, time.time(), token="other"))
            check(inspect_lock(lock_path)[0] == HELD,
                  "a lock owned by another live process is held")
        finally:
            sleeper.kill()
            sleeper.wait()
        stale_error = caught(RunLock(lock_path).acquire, LockError)
        check(stale_error is not None and stale_error.verdict == STALE
              and stale_error.exit_code == 4,
              "a dead run's lock is refused with exit code 4, not skipped  <-- pinned defect")
        check("--apply" in str(stale_error), "and the error says how to check and remove it")
        check(os.path.exists(lock_path),
              "acquiring never deletes a stale lock itself  <-- pinned defect")

        def naive_guard(path):
            """The guard this replaces: exists means 'already running', exit 0."""
            return 0 if os.path.exists(path) else None
        check(naive_guard(lock_path) == 0,
              "the exists-then-exit-0 guard reports success on the same dead lock")

        code, out = cli(["--lock", lock_path])
        check(code == 4 and out.startswith("STALE"), "--lock on a stale lock prints STALE, exits 4")
        check(os.path.exists(lock_path) and "Nothing was removed" in out,
              "--lock without --apply removes nothing")
        code, out = cli(["--lock", lock_path, "--apply"])
        check(code == 0 and not os.path.exists(lock_path) and "Removed" in out,
              "--lock --apply removes a stale lock and exits 0")
        code, out = cli(["--lock", lock_path])
        check(code == 0 and out.startswith("FREE"), "--lock with no lock file prints FREE, exits 0")

        with RunLock(lock_path):
            code, out = cli(["--lock", lock_path, "--apply"])
            check(code == 3 and os.path.exists(lock_path) and "never removed" in out,
                  "--apply never removes a held lock  <-- pinned defect")
        _touch(lock_path, body=_lock_text(4242, "far-host", time.time() - 7200))
        check(cli(["--lock", lock_path])[0] == 3,
              "another host's lock with no --max-age exits 3")
        check(cli(["--lock", lock_path, "--max-age", "3600"])[0] == 4,
              "another host's lock past --max-age exits 4")

        real_inspect = inspect_lock
        answers = [(STALE, "old", "A"), (STALE, "old", "B")]
        globals()["inspect_lock"] = lambda path, max_age=None: answers.pop(0)
        try:
            code, out = cli(["--lock", lock_path, "--apply"])
        finally:
            globals()["inspect_lock"] = real_inspect
        check(code == 4 and os.path.exists(lock_path) and "changed" in out,
              "--apply removes nothing if the lock changed between check and remove")
        globals()["inspect_lock"] = lambda path, max_age=None: (FREE, "gone", None)
        try:
            race = caught(RunLock(lock_path).acquire, LockError)
        finally:
            globals()["inspect_lock"] = real_inspect
        check(race is not None and race.verdict == HELD,
              "a lock released between create and read is reported held, try again")

        rel_log, rel_capture = quiet_logger("jobharness.selftest.release")
        mine_lock = RunLock(os.path.join(lock_dir, "swap.lock"), logger=rel_log).acquire()
        _touch(mine_lock.path, body=_lock_text(4242, here, time.time(), token="someone-else"))
        mine_lock.release()
        check(os.path.exists(mine_lock.path),
              "release() leaves a lock that now holds another run's token  <-- pinned defect")
        gone_lock = RunLock(os.path.join(lock_dir, "gone.lock"), logger=rel_log).acquire()
        os.remove(gone_lock.path)
        gone_lock.release()
        check(any("now belongs to another run" in r for r in rel_capture.records)
              and any("already gone" in r for r in rel_capture.records),
              "both odd releases are logged as warnings")
        check(inspect_lock(lock_dir)[0] == HELD,
              "a lock path that cannot be read is held, never stale")
        raises(lambda: RunLock(lock_path, max_age=-1), "a negative max age is a ValueError")
        _touch(lock_path, body="")
        os.utime(lock_path, (time.time() - 3600, time.time() - 3600))
        check(inspect_lock(lock_path)[0] == STALE, "an empty lock file an hour old is stale")

        # --- command line -----------------------------------------------------
        code, _out = cli(["--lock", lock_path, "--ap"])
        check(code == 2, "a unique prefix --ap of --apply is REFUSED by the parser  <-- pinned defect")
        check(cli(["--lock", lock_path, "--max-a", "5"])[0] == 2,
              "a unique prefix --max-a of --max-age is refused too")
        check(cli(["--apply"])[0] == 2, "--apply without --lock is a usage error")
        check(cli(["--lock", lock_path, "--max-age", "-1"])[0] == 2,
              "a negative --max-age is a usage error")
        check(_parse(["--lock", "x"]).apply is False, "--apply defaults to OFF")
        code, out = cli([])
        check(code == 0 and "--self-test" in out, "no arguments prints the module help")
        argv_before = sys.argv
        try:
            sys.argv = ["jobharness.py", "--lock", os.path.join(lock_dir, "absent.lock")]
            check(cli(None)[0] == 0, "main with no argv reads the arguments after the program name")
        finally:
            sys.argv = argv_before

    finally:
        logging.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)

    # ---- the harness itself. A check() that cannot record a failure would
    # report every defect above as a pass. The probe's own output is swallowed.
    out = sys.stdout
    sys.stdout = io.StringIO()
    mark = len(failed)
    try:
        check(False, "probe: a false condition must be recorded as a failure")
        raises(lambda: None, "probe: a call that raises nothing must fail")
        raises(lambda: [][0], "probe: the wrong exception must fail")
        probe_report = report()
    finally:
        sys.stdout = out
    probe = failed[mark:]
    del failed[mark:]
    check(len(probe) == 3 and probe_report == 1,
          "check() and raises() really do record a failure  <-- pinned defect")
    check(caught(lambda: None) is None, "caught() returns None when nothing is raised")

    return report()


if __name__ == "__main__":
    sys.exit(main())
