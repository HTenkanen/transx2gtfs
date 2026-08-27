"""
Logging of the conversion: the ``transx2gtfs`` logger, a console handler used
when nobody configured logging, an optional log file, and a mirror that also
writes the data warnings (``warnings.warn``) into the log.
"""

import logging
import sys
import warnings

LOGGER_NAME = "transx2gtfs"
log = logging.getLogger(LOGGER_NAME)
MIRRORED = "transx2gtfs_mirrored_warning"


def ensure_info():
    """Let INFO records through; an application's finer level is kept"""
    if log.level == logging.NOTSET or log.level > logging.INFO:
        log.setLevel(logging.INFO)


def _not_mirrored(record):
    """Console filter: mirrored warnings are already shown by the warnings module"""
    return not getattr(record, MIRRORED, False)


def configure_console():
    """
    Show INFO messages on stdout unless logging was configured already (a
    handler on this logger or on the root logger); the logger level is set to
    INFO only when this handler is added (a finer level is kept).
    """
    if not log.handlers and not logging.getLogger().handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler.addFilter(_not_mirrored)
        handler.transx2gtfs_console = True
        log.addHandler(handler)
        ensure_info()


def add_file_handler(path):
    """
    Append every message (with time and level) to ``path``; returns the
    handler. The logger level is set to INFO so that the progress reaches the
    file. A handler for the same path that is already attached (inherited by a
    forked worker) is reused.
    """
    for handler in log.handlers:
        if getattr(handler, "transx2gtfs_file", None) == path:
            return handler
    handler = logging.FileHandler(
        path, mode="a", encoding="utf-8", errors="backslashreplace"
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    handler.transx2gtfs_file = path
    log.addHandler(handler)
    ensure_info()
    return handler


def remove_file_handler(handler):
    log.removeHandler(handler)
    handler.close()


class mirror_warnings:
    """
    Context: the warnings the filters let through are logged (WARNING, tagged
    so that the package's console handler skips them) as well as shown. Nested
    use, or a mirror inherited by a forked worker, installs nothing twice.
    """

    def __enter__(self):
        self._previous = warnings.showwarning
        if getattr(self._previous, MIRRORED, False):
            self._installed = False
            return self

        def show(message, category, filename, lineno, file=None, line=None):
            # Without any handler (a spawned worker of an application that
            # configured logging itself) the warning is shown once, by the
            # warnings module, instead of twice via logging's last resort
            if log.hasHandlers():
                log.warning(
                    "%s: %s", category.__name__, message, extra={MIRRORED: True}
                )
            self._previous(message, category, filename, lineno, file, line)

        setattr(show, MIRRORED, True)
        warnings.showwarning = show
        self._installed = True
        return self

    def __exit__(self, *exc):
        if self._installed:
            warnings.showwarning = self._previous
        return False


def console_in_use():
    """Whether the package's own console handler is what shows the messages"""
    return any(getattr(h, "transx2gtfs_console", False) for h in log.handlers)


def configure_worker(log_file=None, console=True):
    """
    Logging inside a worker process: the package console handler only when the
    parent uses it too (a spawned worker inherits no handlers, a forked one
    inherits them), the optional log file, mirrored warnings.
    """
    if console:
        configure_console()
    if log_file:
        add_file_handler(log_file)
    mirror_warnings().__enter__()
