import importlib
import os
import time
import sys
from typing import Any

import pytest
from attrs import define
from hypothesis import given, settings, strategies as st, Verbosity
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    rule,
    consumes,
    precondition,
)

from cog.server.eventtypes import Log, Done, PredictionOutput, PredictionOutputType
from cog.server.exceptions import InvalidStateException, FatalWorkerException
from cog.server.worker import Worker

settings.register_profile("ci", max_examples=1000, deadline=300)
settings.register_profile("default", max_examples=100, deadline=300)
settings.register_profile("quick", max_examples=10, deadline=300)
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "default"))


SETUP_FATAL_FIXTURES = [
    ("exc_in_setup", {}),
    ("exc_in_setup_and_predict", {}),
    ("exc_on_import", {}),
    ("exit_in_setup", {}),
    ("exit_on_import", {}),
    ("missing_predictor", {}),
    ("missing_setup", {}),
    ("nonexistent_file", {}),  # this fixture doesn't even exist
]

PREDICTION_FATAL_FIXTURES = [
    ("exit_in_predict", {}),
    ("killed_in_predict", {}),
]

RUNNABLE_FIXTURES = [
    ("simple", {}),
    ("exc_in_predict", {}),
    ("missing_predict", {}),
]

OUTPUT_FIXTURES = [
    (
        "hello_world",
        {"name": st.sampled_from(["alice", "bob", "world"])},
        lambda x: f"hello, {x['name']}",
    ),
    (
        "count_up",
        {"upto": st.integers(min_value=0, max_value=1000)},
        lambda x: list(range(x["upto"])),
    ),
    ("complex_output", {}, lambda _: {"number": 42, "text": "meaning of life"}),
]


@define
class Result:
    stdout: str
    stderr: str
    output_type: PredictionOutputType
    output: Any
    done: Done


def _process(events):
    stdout = []
    stderr = []
    output_type = None
    output = None
    done = None
    for event in events:
        if isinstance(event, Log) and event.source == "stdout":
            stdout.append(event.message)
        elif isinstance(event, Log) and event.source == "stderr":
            stderr.append(event.message)
        elif isinstance(event, Done):
            assert not done
            done = event
        elif isinstance(event, PredictionOutput):
            assert output_type, "Should get output type before any output"
            if output_type.multi:
                output.append(event.payload)
            else:
                assert output is None, "Should not get multiple outputs for output type single"
                output = event.payload
        elif isinstance(event, PredictionOutputType):
            assert output_type is None, "Should not get multiple output type events"
            output_type = event
            if output_type.multi:
                output = []
        else:
            pytest.fail(f"saw unexpected event: {event}")
    return Result(
        done=done,
        output=output,
        output_type=output_type,
        stdout="".join(stdout),
        stderr="".join(stderr),
    )


def _fixture_path(name):
    test_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(test_dir, f"fixtures/{name}.py") + ":Predictor"


@pytest.mark.parametrize("name,payloads", SETUP_FATAL_FIXTURES)
def test_fatalworkerexception_from_setup_failures(name, payloads):
    w = Worker(predictor_ref=_fixture_path(name))

    with pytest.raises(FatalWorkerException):
        _process(w.setup())

    w.terminate()


@pytest.mark.parametrize("name,payloads", PREDICTION_FATAL_FIXTURES)
@given(data=st.data())
def test_fatalworkerexception_from_irrecoverable_failures(data, name, payloads):
    w = Worker(predictor_ref=_fixture_path(name))

    result = _process(w.setup())
    assert not result.done.error

    with pytest.raises(FatalWorkerException):
        for _ in range(5):
            payload = data.draw(st.fixed_dictionaries(payloads))
            _process(w.predict(**payload))

    w.terminate()


@pytest.mark.parametrize("name,payloads", RUNNABLE_FIXTURES)
@given(data=st.data())
def test_no_exceptions_from_recoverable_failures(data, name, payloads):
    w = Worker(predictor_ref=_fixture_path(name))

    try:
        result = _process(w.setup())
        assert not result.done.error

        for _ in range(5):
            payload = data.draw(st.fixed_dictionaries(payloads))
            _process(w.predict(**payload))
    finally:
        w.terminate()


@pytest.mark.parametrize("name,payloads,output_generator", OUTPUT_FIXTURES)
@given(data=st.data())
def test_output(data, name, payloads, output_generator):
    w = Worker(predictor_ref=_fixture_path(name))

    try:
        result = _process(w.setup())
        assert not result.done.error

        payload = data.draw(st.fixed_dictionaries(payloads))
        expected_output = output_generator(payload)

        result = _process(w.predict(**payload))

        assert result.output == expected_output
    finally:
        w.terminate()



names = st.sampled_from(["John", "Barry", "Elspeth", "Hamid", "Ronnie", "Yasmeen"])


class WorkerState(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.worker = Worker(_fixture_path("hello_world"))

    cancel_sent = False

    setup_result = Bundle("setup_result")
    predict_result = Bundle("predict_result")

    @rule(target=setup_result)
    def setup(self):
        try:
            return self.worker.setup()
        except InvalidStateException as e:
            return e

    @rule(r=consumes(setup_result))
    def setup_result_valid(self, r):
        if isinstance(r, InvalidStateException):
            return

        result = _process(r)

        assert result.stdout == "did setup\n"
        assert result.stderr == ""
        assert result.done == Done()

    @rule(target=predict_result, name=st.one_of(st.text(), names))
    def predict(self, name):
        try:
            events = self.worker.predict(name=name)
            self.cancel_sent = False
            return events
        except InvalidStateException as e:
            return e

    @precondition(lambda self: not self.cancel_sent)
    @rule(r=consumes(predict_result))
    def predict_result_valid(self, r):
        if isinstance(r, InvalidStateException):
            return

        result = _process(r)

        assert result.stdout.startswith("hello, ")
        assert result.stdout.endswith("\n")
        assert result.stderr == ""
        assert result.done == Done()

    @precondition(lambda self: self.cancel_sent)
    @rule(r=consumes(predict_result))
    def predict_result_when_cancelled(self, r):
        if isinstance(r, InvalidStateException):
            return

        result = _process(r)

        assert result.done == Done(cancelled=True)

    # @rule()
    # def cancel(self):
    #     try:
    #         self.worker.cancel()
    #         self.cancel_sent = True
    #     except InvalidStateException:
    #         return

    def teardown(self):
        self.worker.terminate()


TestWorkerState = WorkerState.TestCase
