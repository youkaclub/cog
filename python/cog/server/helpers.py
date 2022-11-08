import signal
import sys
from contextlib import AbstractContextManager, contextmanager


class ConnectionStream:
    def __init__(self, conn, ctor, **kwargs):
        self.conn = conn
        self.ctor = ctor
        self.kwargs = kwargs

    def write(self, message):
        size = len(message)
        self.conn.send(self.ctor(message, **self.kwargs))
        return size

    def flush(self):
        pass


class redirect_streams(AbstractContextManager):
    def __init__(self, new_stdout, new_stderr):
        self._new_stdout = new_stdout
        self._new_stderr = new_stderr
        self._old_stdouts = []
        self._old_stderrs = []

    def __enter__(self):
        self._old_stdouts.append(sys.stdout)
        self._old_stderrs.append(sys.stderr)
        sys.stdout = self._new_stdout
        sys.stderr = self._new_stderr
        return self._new_stdout, self._new_stderr

    def __exit__(self, exctype, excinst, exctb):
        sys.stdout = self._old_stdouts.pop()
        sys.stderr = self._old_stderrs.pop()
