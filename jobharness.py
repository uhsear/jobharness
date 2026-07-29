#!/usr/bin/env python3
"""jobharness - one import gives a scheduled script logging, retry, resume, and a safe unzip.

A convenience layer over the standard library for Python scripts that run
unattended on a scheduler (cron, Windows Task Scheduler, systemd timers).
Nothing here is novel. Each helper is a thin, tested wrapper over a stdlib
call, written down once so a fleet of scripts stops re-deriving it:

    setup_logging   -> logging.FileHandler + logging.StreamHandler + glob/unlink purge
    safe_extract    -> zipfile.ZipFile.extract behind a resolved-path containment check
    retry           -> functools.wraps + time.sleep
    EmailNotifier   -> smtplib.SMTP
    Checkpoints     -> json
    FTPSession      -> ftplib.FTP_TLS  (adopted public workaround, see class docstring)
    run_summary     -> str.format

Python 3.8+. Standard library only, no optional third-party imports.
Import time touches nothing: no filesystem, no network, no clock.

Run the offline self-test:

    python jobharness.py --self-test
"""

from __future__ import annotations

import datetime as _dt
import ftplib
import functools
import json
import logging
import os
import re
import shutil
import smtplib
import sys
import time
import zipfile
from email.message import EmailMessage

__version__ = "1.0.0"
__all__ = [
    "setup_logging",
    "safe_extract",
    "UnsafeArchiveError",
    "retry",
    "EmailNotifier",
    "Checkpoints",
    "FTPSession",
    "run_summary",
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
    log_path = os.path.join(log_dir, "{}_{}.log".format(name, started.strftime(_TIMESTAMP_FMT)))

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
                        reason = "collides with earlier member {!r}".format(seen[key])
                    seen[key] = member.filename
            if reason is not None:
                raise UnsafeArchiveError(
                    "refusing {!r} from {}: {}".format(member.filename, zip_path, reason)
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
            last = None
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last = exc
                    if attempt == attempts:
                        break
                    if on_retry is not None:
                        on_retry(exc, attempt)
                    sleep(delay * (backoff ** (attempt - 1)))
            raise last
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
        return "EmailNotifier(host={!r}, port={!r}, user={!r}, password_env={!r}, use_tls={!r})".format(
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
                    "environment variable {!r} is unset or empty".format(self.password_env)
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
# 5. Checkpoints
# ---------------------------------------------------------------------------

class Checkpoints:
    """Record which named steps finished, so a re-run resumes instead of redoing.

    Wraps json. State is a small dict written atomically (temp file +
    os.replace) so a crash mid-write cannot leave a half-file. A file that is
    already corrupt or unreadable is reported and treated as "nothing done
    yet". A resume aid must never be the thing that stops the job.

    Honest note: unlike the other helpers in this module, this pattern had
    exactly ONE prior use in the corpus that motivated jobharness. It is
    generalized from a single site, not from a repeated one.
    """

    def __init__(self, path, logger=None):
        self.path = path
        self._log = logger or logging.getLogger(__name__)
        self.state = self._load()

    def _load(self):
        blank = {"completed_steps": [], "last_run": None, "data": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
        except FileNotFoundError:
            return blank
        except (ValueError, OSError, UnicodeDecodeError) as exc:
            self._log.warning("checkpoint file %s unreadable (%s); starting fresh", self.path, exc)
            return blank
        if not isinstance(loaded, dict) or not isinstance(loaded.get("completed_steps"), list):
            self._log.warning("checkpoint file %s has unexpected shape; starting fresh", self.path)
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

    def is_done(self, step):
        return step in self.state["completed_steps"]

    def mark_done(self, step, **data):
        if step not in self.state["completed_steps"]:
            self.state["completed_steps"].append(step)
        if data:
            self.state["data"][step] = data
        self.state["last_run"] = _dt.datetime.now().isoformat(timespec="seconds")
        self.save()

    def get(self, step, default=None):
        return self.state["data"].get(step, default)

    def clear(self):
        """Forget all recorded steps and remove the state file."""
        self.state = {"completed_steps": [], "last_run": None, "data": {}}
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
    lines = [rule, "RUN SUMMARY: {}".format(name), rule,
             "Started  : {}".format(started.strftime(LOG_DATEFMT)),
             "Finished : {}".format(finished.strftime(LOG_DATEFMT)),
             "Elapsed  : {}".format(_dt.timedelta(seconds=int((finished - started).total_seconds()))),
             "Result   : {}".format("FAILED ({} error(s))".format(len(errors)) if errors else "OK")]
    for key, value in (extra or {}).items():
        lines.append("{:<9}: {}".format(str(key)[:9], value))
    if steps:
        lines.append("-" * width)
        for step in steps:
            lines.append("  step: {}".format(step))
    if errors:
        lines.append("-" * width)
        for err in errors:
            lines.append("  ERROR: {}".format(err))
    lines.append(rule)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Offline self-test. No network, no credentials, no real clock dependence.
# ---------------------------------------------------------------------------

class _Counter:
    def __init__(self):
        self.n = 0

    def __call__(self, cond, label):
        self.n += 1
        if not cond:
            raise AssertionError("FAIL [{}]: {}".format(self.n, label))


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


def _self_test():
    import tempfile
    import warnings

    ok = _Counter()
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
            raise ValueError("boom {}".format(calls["n"]))

        try:
            always_fails()
            raised = None
        except ValueError as exc:
            raised = exc
        ok(raised is not None, "retry re-raises after exhaustion")
        ok(str(raised) == "boom 4", "retry re-raises the LAST exception, not the first")
        ok(calls["n"] == 4, "retry made exactly `attempts` calls")
        ok(slept == [0.5, 1.0, 2.0], "backoff sequence is delay*backoff**n: got {}".format(slept))
        ok(len(slept) == 3, "no sleep after the final attempt")
        ok([a for a, _ in seen] == [1, 2, 3], "on_retry fires with 1-based attempt numbers")
        ok(seen[0][1] == "boom 1", "on_retry receives the exception that just failed")

        succeeded = {"n": 0}

        @retry(ValueError, attempts=3, delay=0, sleep=slept.append)
        def fails_once():
            succeeded["n"] += 1
            if succeeded["n"] < 2:
                raise ValueError("transient")
            return "value"

        ok(fails_once() == "value", "retry returns the successful result")
        ok(succeeded["n"] == 2, "retry stops calling once the function succeeds")

        unlisted = {"n": 0}

        @retry(ValueError, attempts=5, delay=0, sleep=slept.append)
        def wrong_exception():
            unlisted["n"] += 1
            raise KeyError("not in the list")

        try:
            wrong_exception()
            escaped = False
        except KeyError:
            escaped = True
        ok(escaped, "an unlisted exception propagates")
        ok(unlisted["n"] == 1, "an unlisted exception is NOT retried")

        @retry((ValueError, TypeError), attempts=2, delay=0, sleep=slept.append)
        def named():
            """docstring survives"""
            raise TypeError("t")

        ok(named.__name__ == "named", "functools.wraps preserves __name__")
        ok(named.__doc__ == "docstring survives", "functools.wraps preserves __doc__")

        # --- 2. safe_extract --------------------------------------------------
        zips = os.path.join(tmp, "zips")
        os.makedirs(zips)

        clean = _make_zip(os.path.join(zips, "clean.zip"),
                          [("a.txt", "alpha"), ("sub/b.txt", "beta")])
        dest = os.path.join(tmp, "out_clean")
        got = safe_extract(clean, dest)
        ok(len(got) == 2, "safe_extract returns one path per file member")
        ok(all(os.path.isfile(p) for p in got), "returned paths exist on disk")
        ok(os.path.abspath(got[0]).startswith(os.path.abspath(dest)),
           "extracted files land inside dest")
        with open(os.path.join(dest, "sub", "b.txt"), encoding="utf-8") as handle:
            ok(handle.read() == "beta", "nested member content is intact")

        for label, member in [
            ("../ traversal", "../escape.txt"),
            ("deep ../ traversal", "sub/../../escape.txt"),
            ("backslash traversal", "..\\escape.txt"),
            ("posix absolute path", "/etc/escape.txt"),
            ("drive-letter path", "C:/Windows/escape.txt"),
            ("drive-relative path", "C:escape.txt"),
            ("UNC-ish path", "\\\\server\\share\\escape.txt"),
        ]:
            bad = _make_zip(os.path.join(zips, "bad_{}.zip".format(abs(hash(member)))),
                            [("ok.txt", "fine"), (member, "pwned")])
            bad_dest = os.path.join(tmp, "out_bad_{}".format(abs(hash(member))))
            try:
                safe_extract(bad, bad_dest)
                refused = False
            except UnsafeArchiveError:
                refused = True
            ok(refused, "safe_extract refuses {}".format(label))
            ok(not os.path.exists(bad_dest),
               "refusing {} leaves NOTHING on disk (dest not created)".format(label))

        escaped_file = os.path.join(tmp, "escape.txt")
        ok(not os.path.exists(escaped_file), "no member ever escaped to the parent directory")

        empty_zip = _make_zip(os.path.join(zips, "empty.zip"), [])
        empty_dest = os.path.join(tmp, "out_empty")
        ok(safe_extract(empty_zip, empty_dest) == [], "an empty archive extracts to an empty list")
        ok(os.path.isdir(empty_dest), "a clean (even empty) archive creates dest")

        ok(_reject_reason("sub/ok.txt") is None, "an ordinary nested name is accepted")
        ok(_reject_reason("..") is not None, "a bare '..' member is rejected")
        ok(_reject_reason("a..b/ok.txt") is None, "'..' inside a filename is not traversal")

        # The name check and the resolved-path check are two independent layers.
        # Neuter the name check and the containment check must still hold, or
        # deleting the containment block would be a silent no-op.
        real_reject = _reject_reason
        globals()["_reject_reason"] = lambda member_name: None
        try:
            sneak = _make_zip(os.path.join(zips, "nameclean.zip"), [("../escape2.txt", "pwned")])
            sneak_dest = os.path.join(tmp, "out_nameclean")
            try:
                safe_extract(sneak, sneak_dest)
                refused = False
            except UnsafeArchiveError:
                refused = True
        finally:
            globals()["_reject_reason"] = real_reject
        ok(refused, "containment refuses even when the NAME check is bypassed")
        ok(not os.path.exists(os.path.join(tmp, "escape2.txt")),
           "the bypassed-name member never reached the parent directory")

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
            try:
                safe_extract(linked, link_dest)
                refused = False
            except UnsafeArchiveError:
                refused = True
            ok(refused, "a symlinked subdirectory of dest is refused (realpath, not abspath)")
            ok(not os.path.exists(os.path.join(outside, "pwned.txt")),
               "nothing was written through the symlink")
            # ...and the mirror case: a dest that IS a symlink is the caller's own
            # choice of directory and must still extract, not trip containment.
            sym_dest = os.path.join(tmp, "out_symdest")
            os.symlink(os.path.join(tmp, "outside_target"), sym_dest, target_is_directory=True)
            ok(len(safe_extract(clean, sym_dest)) == 2,
               "a dest that is itself a symlink still extracts (no false refusal)")
        else:
            ok(True, "symlink containment: SKIPPED, this platform/user cannot create symlinks")
            ok(True, "symlink containment: SKIPPED (2 of 3)")
            ok(True, "symlink containment: SKIPPED (3 of 3)")

        # Two members writing one path: the second silently overwrites the first
        # under a plain extractall, and the returned list would claim both.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # zipfile warns about the duplicate name
            dup = _make_zip(os.path.join(zips, "dup.zip"),
                            [("config.txt", "real"), ("config.txt", "EVIL")])
        try:
            safe_extract(dup, os.path.join(tmp, "out_dup"))
            refused = False
        except UnsafeArchiveError:
            refused = True
        ok(refused, "two members resolving to the same path are refused")

        cased = _make_zip(os.path.join(zips, "cased.zip"),
                          [("config.txt", "real"), ("CONFIG.TXT", "EVIL")])
        cased_dest = os.path.join(tmp, "out_cased")
        try:
            safe_extract(cased, cased_dest)
            refused = False
        except UnsafeArchiveError:
            refused = True
        collides = os.path.normcase("config.txt") == os.path.normcase("CONFIG.TXT")
        ok(refused == collides,
           "a case-only duplicate is refused exactly where the filesystem collides")

        # Containment-clean but unwritable: "sub" as a file, then "sub/x.txt".
        clash = _make_zip(os.path.join(zips, "clash.zip"),
                          [("sub", "i am a file"), ("sub/x.txt", "boom")])
        clash_dest = os.path.join(tmp, "out_clash")
        try:
            safe_extract(clash, clash_dest)
            failed = False
        except (UnsafeArchiveError, OSError):
            failed = True
        ok(failed, "a member set that cannot be written raises instead of half-succeeding")
        ok(not os.path.exists(clash_dest), "a failed extraction leaves no half-written dest")

        # --- 1. setup_logging -------------------------------------------------
        logs = os.path.join(tmp, "logs")
        logger, path = setup_logging("jobA", logs, console=False)
        ok(os.path.isfile(path), "setup_logging creates the log file")
        ok(_log_pattern("jobA").match(os.path.basename(path)),
           "log filename is <name>_YYYYmmdd_HHMMSS.log")
        logger.info("hello from the self-test")
        for handler in logger.handlers:
            handler.flush()
        with open(path, encoding="utf-8") as handle:
            body = handle.read()
        ok("hello from the self-test" in body, "setup_logging writes records to the file")
        ok("INFO" in body, "the file handler uses the module formatter")

        try:
            setup_logging("jobA", logs, retain_days=7, retain_count=5, console=False)
            both = False
        except ValueError:
            both = True
        ok(both, "passing two retention policies is a ValueError")
        for bad_kwargs in ({"retain_days": 0}, {"retain_count": 0}):
            try:
                setup_logging("jobA", logs, console=False, **bad_kwargs)
                guarded = False
            except ValueError:
                guarded = True
            ok(guarded, "a non-positive retention value is rejected: {}".format(bad_kwargs))

        # Destructive default OFF.
        off_dir = os.path.join(tmp, "logs_off")
        os.makedirs(off_dir)
        ancient = time.time() - 400 * 86400
        _touch(os.path.join(off_dir, "jobB_20200101_000000.log"), ancient)
        _touch(os.path.join(off_dir, "jobB_20200102_000000.log"), ancient)
        setup_logging("jobB", off_dir, console=False)
        ok(len(os.listdir(off_dir)) == 3,
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
        _log_c, path_c = setup_logging("jobC", age_dir, retain_days=7, console=False)
        ok(not os.path.exists(old), "prune-by-age deletes a log older than the cutoff")
        ok(os.path.exists(mid), "prune-by-age keeps a log newer than the cutoff")
        ok(os.path.exists(new), "prune-by-age keeps the newest log")
        ok(os.path.exists(path_c), "prune-by-age never deletes the log it just opened")
        ok(os.path.exists(other_job), "pruning never touches ANOTHER job's logs")
        ok(os.path.exists(not_a_log), "pruning never touches a non-timestamped .log file")
        ok(os.path.exists(unrelated), "pruning never touches an unrelated file")

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
        ok(os.path.exists(on_cutoff), "prune-by-age KEEPS a log whose mtime is exactly the cutoff")
        ok(not os.path.exists(past_cutoff), "prune-by-age deletes one second past the cutoff")

        # Prune by count.
        cnt_dir = os.path.join(tmp, "logs_count")
        os.makedirs(cnt_dir)
        for i in range(1, 6):
            _touch(os.path.join(cnt_dir, "jobE_2020010{}_000000.log".format(i)),
                   now_ts - (10 - i) * day)
        decoy_job = _touch(os.path.join(cnt_dir, "jobF_20200101_000000.log"), now_ts - 99 * day)
        decoy_name = _touch(os.path.join(cnt_dir, "jobE_backup.log"), now_ts - 99 * day)
        _log_e, path_e = setup_logging("jobE", cnt_dir, retain_count=3, console=False)
        kept = sorted(f for f in os.listdir(cnt_dir) if _log_pattern("jobE").match(f))
        ok(len(kept) == 3, "prune-by-count keeps exactly N: got {}".format(kept))
        ok(os.path.basename(path_e) in kept, "the log just opened counts toward N and survives")
        ok("jobE_20200101_000000.log" not in kept, "prune-by-count drops the oldest first")
        ok("jobE_20200105_000000.log" in kept, "prune-by-count keeps the newest of the old logs")
        ok(os.path.exists(decoy_job), "prune-by-count never touches another job's logs")
        ok(os.path.exists(decoy_name), "prune-by-count never touches a non-timestamped .log file")

        for lg in (logger, _log_c, _log_e, _log_g, logging.getLogger("jobB")):
            for handler in list(lg.handlers):
                lg.removeHandler(handler)
                handler.close()

        # --- 5. Checkpoints ---------------------------------------------------
        cp_path = os.path.join(tmp, "state", "checkpoints.json")
        cp = Checkpoints(cp_path)
        ok(cp.is_done("download") is False, "a fresh Checkpoints reports nothing done")
        cp.mark_done("download", rows=1234)
        ok(os.path.isfile(cp_path), "mark_done writes the state file")
        ok(cp.is_done("download"), "mark_done records the step")

        resumed = Checkpoints(cp_path)
        ok(resumed.is_done("download"), "a new Checkpoints round-trips completed steps")
        ok(resumed.get("download") == {"rows": 1234}, "per-step data round-trips through json")
        ok(resumed.is_done("publish") is False, "an unrecorded step is not marked done")

        ran = []
        for step in ("download", "transform", "publish"):
            if resumed.is_done(step):
                continue
            ran.append(step)
            resumed.mark_done(step)
        ok(ran == ["transform", "publish"], "resume skips the completed step")
        ok(len(resumed.state["completed_steps"]) == 3, "no duplicate step entries")
        resumed.mark_done("publish")
        ok(len(resumed.state["completed_steps"]) == 3, "mark_done is idempotent")

        corrupt_path = os.path.join(tmp, "corrupt.json")
        _touch(corrupt_path, body="{not json at all,,,")
        quiet = logging.getLogger("jobharness.selftest.corrupt")
        cp_capture = _RecordingHandler()
        cp_capture.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        quiet.handlers = [cp_capture]
        quiet.propagate = False
        quiet.setLevel(logging.DEBUG)
        recovered = Checkpoints(corrupt_path, logger=quiet)
        ok(recovered.state["completed_steps"] == [], "a corrupt state file recovers as empty")
        ok(any("unreadable" in rec for rec in cp_capture.records),
           "a corrupt state file is REPORTED, not silently swallowed")
        ok(any(rec.startswith("WARNING") for rec in cp_capture.records),
           "the corrupt-file report is a warning, not a debug line")
        recovered.mark_done("after_corruption")
        ok(Checkpoints(corrupt_path, logger=quiet).is_done("after_corruption"),
           "a recovered Checkpoints can still save")

        wrong_shape = os.path.join(tmp, "wrongshape.json")
        _touch(wrong_shape, body='["a", "list", "not", "a", "dict"]')
        cp_capture.records = []
        ok(Checkpoints(wrong_shape, logger=quiet).state["completed_steps"] == [],
           "an unexpected JSON shape recovers as empty")
        ok(any("unexpected shape" in rec for rec in cp_capture.records),
           "an unexpected JSON shape is REPORTED, not silently swallowed")

        cp.clear()
        ok(not os.path.exists(cp_path), "clear() removes the state file")
        ok(cp.is_done("download") is False, "clear() forgets completed steps")
        cp.clear()
        ok(True, "clear() on a missing file does not raise")

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
            ok(len(_FakeSMTP.instances) == 1, "a clean send opens exactly one connection")
            first = _FakeSMTP.instances[0]
            ok(len(first.sent) == 1, "a clean send calls sendmail exactly once")
            ok(first.tls is True, "use_tls issues STARTTLS")
            ok(first.logins == [("svc-account", fake_password)],
               "the password is read from the named environment variable")
            ok("subject one" in first.sent[0][2], "the subject reaches the wire")
            ok(first.sent[0][1] == ["ops@example.invalid"], "recipients reach the wire")

            first.fail_next_send = True
            notifier.send("subject two", "body two", ["ops@example.invalid"],
                          "jobs@example.invalid")
            ok(len(_FakeSMTP.instances) == 2, "SMTPServerDisconnected triggers exactly one reconnect")
            second = _FakeSMTP.instances[1]
            ok(len(second.sent) == 1, "the message is re-sent once on the new connection")
            ok(first.quit_count == 1, "the dead connection is closed before reconnecting")

            # Third send, nothing wrong with the socket: reconnecting here would
            # be indistinguishable from the reconnect above with only two sends.
            notifier.send("subject three", "body three", "ops@example.invalid",
                          "jobs@example.invalid")
            ok(len(_FakeSMTP.instances) == 2,
               "a later clean send REUSES the connection instead of reconnecting")
            ok(len(second.sent) == 2, "the third message went out on the existing connection")

            ok(fake_password not in repr(notifier), "the password is not in repr()")
            ok(fake_password not in str(vars(notifier)), "the password is not stored on the instance")
            ok(all(fake_password not in rec for rec in capture.records),
               "the password never reaches a log record")
            ok(any("reconnect" in rec.lower() for rec in capture.records),
               "the reconnect is logged")
            ok(notifier.password_env == fake_env, "only the env var NAME is retained")

            try:
                notifier.send("s", "b", [], "jobs@example.invalid")
                empty_guard = False
            except ValueError:
                empty_guard = True
            ok(empty_guard, "sending with no recipients is a ValueError")

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
            try:
                bad_login.connect()
                login_raised = False
                frames = []
            except smtplib.SMTPAuthenticationError as exc:
                login_raised = True
                frames = []
                tb = exc.__traceback__
                while tb is not None:
                    frames.append(tb.tb_frame)
                    tb = tb.tb_next
            ok(login_raised, "a failed login propagates")
            ours = [f for f in frames if f.f_code.co_name == "connect"]
            ok(len(ours) == 1, "the traceback contains EmailNotifier.connect's frame")
            ok(all(fake_password not in str(v) for f in ours for v in f.f_locals.values()),
               "a failed login leaves no password in connect()'s frame locals")

            del os.environ[fake_env]
            missing = EmailNotifier("smtp.invalid", 25, user="svc-account",
                                    password_env=fake_env, smtp_factory=_FakeSMTP,
                                    logger=mail_log)
            try:
                missing.connect()
                unset_guard = False
            except RuntimeError:
                unset_guard = True
            ok(unset_guard, "an unset password env var fails loudly, not silently")
        finally:
            os.environ.pop(fake_env, None)

        # --- 6. FTPSession ----------------------------------------------------
        ok("ntransfercmd" in FTPSession.__dict__, "FTPSession overrides ntransfercmd")
        ok("makepasv" in FTPSession.__dict__, "FTPSession overrides makepasv")
        ok(issubclass(FTPSession, ftplib.FTP_TLS), "FTPSession subclasses ftplib.FTP_TLS")
        original = ftplib.FTP_TLS.makepasv
        try:
            ftplib.FTP_TLS.makepasv = lambda self: ("10.0.0.7", 50123)  # private NAT address
            session = FTPSession()  # no host -> ftplib does not connect
            ok(session.sock is None, "constructing FTPSession opens no socket")
            session.host = "files.example.invalid"
            ok(session.makepasv() == ("files.example.invalid", 50123),
               "makepasv returns the ORIGINAL host and the server's port")
        finally:
            ftplib.FTP_TLS.makepasv = original

        # --- 7. run_summary ---------------------------------------------------
        start = _dt.datetime(2024, 3, 1, 8, 0, 0)
        end = _dt.datetime(2024, 3, 1, 8, 42, 30)
        clean_text = run_summary("nightly", start, end, steps=["download", "publish"])
        ok("Elapsed  : 0:42:30" in clean_text, "run_summary computes elapsed time")
        ok("Result   : OK" in clean_text, "run_summary reports OK with no errors")
        ok("step: download" in clean_text, "run_summary lists steps")
        failed_text = run_summary("nightly", start, end, errors=["disk full", "timeout"])
        ok("FAILED (2 error(s))" in failed_text, "run_summary counts errors")
        ok("ERROR: disk full" in failed_text, "run_summary lists errors")

    finally:
        logging.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)

    print("jobharness self-test: {} assertions passed".format(ok.n))
    return 0


if __name__ == "__main__":
    if "--self-test" in sys.argv[1:]:
        sys.exit(_self_test())
    print(__doc__.strip())
    print("\njobharness {}, run with --self-test".format(__version__))
