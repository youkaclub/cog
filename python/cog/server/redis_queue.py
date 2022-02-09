from io import BytesIO
import pika
import json
from pathlib import Path
from typing import Optional
import signal
import sys
import traceback
import time
import types
import contextlib

import redis
import requests
from werkzeug.datastructures import FileStorage

from .redis_log_capture import capture_log
from ..input import InputValidationError, validate_and_convert_inputs
from ..json import to_json
from ..predictor import Predictor, load_predictor


class timeout:
    """A context manager that times out after a given number of seconds."""

    def __init__(self, seconds, elapsed=None, error_message="Prediction timed out"):
        if elapsed is None or seconds is None:
            self.seconds = seconds
        else:
            self.seconds = seconds - int(elapsed)
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        if self.seconds is not None:
            if self.seconds <= 0:
                self.handle_timeout(None, None)
            else:
                signal.signal(signal.SIGALRM, self.handle_timeout)
                signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        if self.seconds is not None:
            signal.alarm(0)


class RedisQueueWorker:
    SETUP_TIME_QUEUE_SUFFIX = "-setup-time"
    RUN_TIME_QUEUE_SUFFIX = "-run-time"
    STAGE_SETUP = "setup"
    STAGE_RUN = "run"

    def __init__(
        self,
        predictor: Predictor,
        redis_host: str,
        redis_port: int,
        input_queue: str,
        upload_url: str,
        consumer_id: str,
        model_id: Optional[str] = None,
        log_queue: Optional[str] = None,
        predict_timeout: Optional[int] = None,
        redis_db: int = 0,
    ):
        self.predictor = predictor
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.input_queue = input_queue
        self.upload_url = upload_url
        self.consumer_id = consumer_id
        self.model_id = model_id
        self.log_queue = log_queue
        self.predict_timeout = predict_timeout
        self.redis_db = redis_db
        # TODO: respect max_processing_time in message handling
        self.max_processing_time = 10 * 60  # timeout after 10 minutes
        self.redis = redis.Redis(
            host=self.redis_host, port=self.redis_port, db=self.redis_db
        )
        self.should_exit = False
        self.setup_time_queue = input_queue + self.SETUP_TIME_QUEUE_SUFFIX
        self.predict_time_queue = input_queue + self.RUN_TIME_QUEUE_SUFFIX
        self.stats_queue_length = 100

        sys.stderr.write(
            f"Connected to Redis: {self.redis_host}:{self.redis_port} (db {self.redis_db})\n"
        )

    def signal_exit(self, signum, frame):
        self.should_exit = True
        sys.stderr.write("Caught SIGTERM, exiting...\n")

    def start(self):
        print('STARTING THE REDIS QUEUE')
        print("STARTING AMQP")
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters('rabbitmq', 5672, '/', credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        keep_running = True  # Just hardcoding for now
        while keep_running:
            try:
                for method_frame, properties, body in channel.consume(
                        'TEST_QUEUE_NAME'):
                    # Display the message parts and acknowledge the message
                    print('Printing message details: ')
                    message_id = properties.message_id
                    print(f'message_id: {message_id}')
                    print(method_frame, properties, body)
                    message = json.loads(body)
                    print(f'PREETHI: message: {message}')
                    response_queue = json.loads(body)['response_queue']
                    # channel.queue_declare(queue=response_queue)
                    cleanup_functions=[]  # This is supposed to be an empty list
                    self.handle_message(channel, response_queue, message, message_id, cleanup_functions)



                    channel.basic_ack(method_frame.delivery_tag)
                time.sleep(1)
            except Exception:
                tb = traceback.format_exc()
                sys.stderr.write(f"Failed to handle message: {tb}\n")

    def handle_message(self, channel, response_queue, message, message_id, cleanup_functions):
        self.predictor.setup()
        inputs = {}
        raw_inputs = message["inputs"]
        prediction_id = message["id"]
        print(f'PREETHI: raw_inputs: {raw_inputs}')
        for k, v in raw_inputs.items():
            if "value" in v and v["value"] != "":
                inputs[k] = v["value"]
            else:
                file_url = v["file"]["url"]
                sys.stderr.write(f"Downloading file from {file_url}\n")
                value_bytes = self.download(file_url)
                inputs[k] = FileStorage(
                    stream=BytesIO(value_bytes), filename=v["file"]["name"]
                )
        try:
            print(f'ALL INPUTS: {inputs}')
            inputs = validate_and_convert_inputs(
                self.predictor, inputs, cleanup_functions
            )
        except InputValidationError as e:
            tb = traceback.format_exc()
            sys.stderr.write(tb)
            self.push_error(channel, response_queue, message_id, e)
            return

        start_time = time.time()
        with self.capture_log(self.STAGE_RUN, prediction_id), timeout(
            seconds=self.predict_timeout
        ):
            return_value = self.predictor.predict(**inputs)
        if isinstance(return_value, types.GeneratorType):
            last_result = None

            while True:
                # we consume iterator manually to capture log
                try:
                    with self.capture_log(self.STAGE_RUN, prediction_id), timeout(
                        seconds=self.predict_timeout, elapsed=time.time() - start_time
                    ):
                        result = next(return_value)
                except StopIteration:
                    break
                # push the previous result, so we can eventually detect the last iteration
                if last_result is not None:
                    print('processing')
                    self.push_result(channel, response_queue, last_result, message_id, status="processing")
                if isinstance(result, Path):
                    cleanup_functions.append(result.unlink)
                last_result = result

            # push the last result
            print('RESULT')
            print(last_result)
            print('success')
            self.push_result(channel, response_queue, last_result, message_id, status="success")
        else:
            if isinstance(return_value, Path):
                cleanup_functions.append(return_value.unlink)
            print('RETURN VALUE')
            print(return_value)
            print('SUCCESS')
            self.push_result(channel, response_queue, return_value, message_id, status="success")

    def download(self, url):
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content

    def push_error(self, channel, response_queue, message_id, error):
        message = {
                "status": "failed",
                "error": str(error),
            }
        sys.stderr.write(f"Pushing error to {response_queue}\n")
        print(f'ERROR MESSAGEID: {message_id}')
        print(f"ERROR MESSAGE: {message}")
        self.send_amqp_message(channel, response_queue, message_id, message)

    def push_result(self, channel,  response_queue, result, message_id, status):
        if isinstance(result, Path):
            message = {
                "file": {
                    "url": self.upload_to_temp(result),
                    "name": result.name,
                }
            }
        elif isinstance(result, str):
            message = {
                "value": result,
            }
        else:
            message = {
                "value": to_json(result),
            }

        message["status"] = status

        sys.stderr.write(f"Pushing successful result to {response_queue}\n")
        self.send_amqp_message(channel, response_queue, message_id, message)
        print('Pushed success result')

    @staticmethod
    def send_amqp_message(channel, response_queue, message_id, message_body):
        print(f'About to send amqp w message_id: {message_id}')
        channel.basic_publish(exchange='',
                              routing_key=response_queue,
                              properties=pika.BasicProperties(message_id=message_id),
                              body=json.dumps(message_body))
        print('Sent AMQP')

    def upload_to_temp(self, path: Path) -> str:
        sys.stderr.write(
            f"Uploading {path.name} to temporary storage at {self.upload_url}\n"
        )
        resp = requests.put(
            self.upload_url, files={"file": (path.name, path.open("rb"))}
        )
        resp.raise_for_status()
        return resp.json()["url"]

    @contextlib.contextmanager
    def capture_log(self, stage, prediction_id):
        with capture_log(
            self.redis_host,
            self.redis_port,
            self.redis_db,
            self.log_queue,
            stage,
            prediction_id,
        ):
            yield


def _queue_worker_from_argv(
    predictor,
    redis_host,
    redis_port,
    input_queue,
    upload_url,
    comsumer_id,
    model_id,
    log_queue,
    predict_timeout=None,
):
    """
    Construct a RedisQueueWorker object from sys.argv, taking into account optional arguments and types.

    This is intensely fragile. This should be kwargs or JSON or something like that.
    """
    if predict_timeout is not None:
        predict_timeout = int(predict_timeout)
    return RedisQueueWorker(
        predictor,
        redis_host,
        redis_port,
        input_queue,
        upload_url,
        comsumer_id,
        model_id,
        log_queue,
        predict_timeout,
    )


if __name__ == "__main__":
    predictor = load_predictor()
    # *sys.argv[1:] is redis_host arg from http serving
    worker = _queue_worker_from_argv(predictor, *sys.argv[1:])
    worker.start()
