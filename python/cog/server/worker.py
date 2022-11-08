import multiprocessing
import os
import signal
import sys
import traceback
import types
from enum import Enum, auto, unique

from ..json import make_encodeable
from .eventtypes import Heartbeat, Done, Log, PredictionInput, PredictionOutput, PredictionOutputType, Shutdown
from .exceptions import CancellationException, InvalidStateException, FatalWorkerException
from .helpers import (
    ConnectionStream,
    convert_signal_to_exception,
    load_predictor,
    redirect_streams,
)

_spawn = multiprocessing.get_context("spawn")


@unique
class WorkerState(Enum):
    NEW = auto()
    STARTING = auto()
    READY = auto()
    PROCESSING = auto()
    DEFUNCT = auto()


class Worker:
    def __init__(self, predictor_ref):
        self._state = WorkerState.NEW

        # A pipe with which to communicate with the child worker.
        self._events, child_events = _spawn.Pipe()
        self._child = _ChildWorker(predictor_ref, child_events)
        self._terminating = False

    def setup(self):
        self._assert_state(WorkerState.NEW)
        self._state = WorkerState.STARTING
        self._child.start()

        return self._wait(raise_on_error="Predictor errored during setup")

    def predict(self, **kwargs):
        self._assert_state(WorkerState.READY)
        self._state = WorkerState.PROCESSING
        self._events.send(PredictionInput(payload=kwargs))

        return self._wait()

    def shutdown(self):
        if self._state != WorkerState.NEW:
            self._terminating = True
            self._events.send(Shutdown())
            self._child.join()
        self._state = WorkerState.DEFUNCT

    def terminate(self):
        if self._state != WorkerState.NEW:
            self._terminating = True
            self._child.terminate()
            self._child.join()
        self._state = WorkerState.DEFUNCT

    def cancel(self):
        os.kill(self._child.pid, signal.SIGUSR1)

    def _assert_state(self, state):
        if self._state != state:
            raise InvalidStateException(
                f"Invalid operation: state is {self._state} (must be {state})"
            )

    def _wait(self, poll=None, raise_on_error=None):
        done = None

        if poll:
            send_heartbeats = True
        else:
            poll = 0.1
            send_heartbeats = False

        while self._child.is_alive() and not done:
            if not self._events.poll(poll):
                if send_heartbeats:
                    self.event.send(Heartbeat())
                continue

            ev = self._events.recv()
            yield ev

            if isinstance(ev, Done):
                done = ev

        if done:
            if done.error and raise_on_error:
                self._state = WorkerState.DEFUNCT
                raise FatalWorkerException(raise_on_error)
            else:
                self._state = WorkerState.READY

        # If we dropped off the end off the end of the loop, check if it's
        # because the child process died.
        if not self._child.is_alive() and not self._terminating:
            exitcode = self._child.exitcode
            raise FatalWorkerException(
                f"Prediction failed for an unknown reason. It might have run out of memory? (exitcode {exitcode})"
            )


class _ChildWorker(_spawn.Process):
    def __init__(self, predictor_ref, events):
        self._predictor_ref = predictor_ref
        self._predictor = None
        self._events = events
        super().__init__()

    def run(self):
        stdout = ConnectionStream(self._events, Log, source="stdout")
        stderr = ConnectionStream(self._events, Log, source="stderr")

        with redirect_streams(stdout, stderr):
            self._setup()
            self._loop()

    def _setup(self):
        done = Done()
        try:
            self._predictor = load_predictor(self._predictor_ref)
            self._predictor.setup()
        except Exception:
            traceback.print_exc()
            done.error = True
        except:  # for SystemExit and friends reraise to ensure the process dies
            traceback.print_exc()
            done.error = True
            raise
        finally:
            self._events.send(done)

    def _loop(self):
        while True:
            done = Done()

            try:
                ev = self._events.recv()
                if isinstance(ev, Shutdown):
                    break
                elif isinstance(ev, PredictionInput):
                    self._predict(done, **ev.payload)
                else:
                    print(f"Got unexpected event: {ev}", file=sys.stderr)
            except EOFError:
                done.error = True
                raise RuntimeError("Connection to Worker unexpectedly closed.")
            finally:
                self._events.send(done)

    def _predict(self, done, **kwargs):
        try:
            with convert_signal_to_exception(signal.SIGUSR1, CancellationException):
                result = self._predictor.predict(**kwargs)
                if result:
                    if isinstance(result, types.GeneratorType):
                        self._events.send(PredictionOutputType(multi=True))
                        for r in result:
                            self._events.send(
                                PredictionOutput(payload=make_encodeable(r))
                            )
                    else:
                        self._events.send(PredictionOutputType(multi=False))
                        self._events.send(
                            PredictionOutput(payload=make_encodeable(result))
                        )
        except CancellationException:
            done.cancelled = True
        except Exception:
            traceback.print_exc()
            done.error = True
