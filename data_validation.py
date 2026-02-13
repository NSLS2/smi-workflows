from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
from bluesky_tiled_plugins.writing.validator import validate
import time as ttime
from tiled.client import from_profile


@task
def check_stream(run):
    logger = get_run_logger()
    for stream in run:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")


@task(retries=2, retry_delay_seconds=10)
def validate_local(run_client):
    logger = get_run_logger()
    validate(run_client, fix_errors=True, try_reading=True, raise_on_error=True)

@flow
def data_validation(uid, beamline_acronym="smi"):
    logger = get_run_logger()
    api_key = Secret.load("tiled-smi-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2", api_key=api_key)
    run_client = tiled_client[beamline_acronym]["migration"][uid]
    run_client_raw = tiled_client[beamline_acronym]["raw"][uid]
    logger.info(f"Launching tasks to check streams and validate uid {uid}")
    start_time = ttime.monotonic()
    check_stream_task = check_stream.submit(run_client_raw)
    validate_task = validate_local.submit(run_client)
    logger.info("Waiting for tasks to complete")
    check_stream_task.result()
    validate_task.result()
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"Finished checking and validating data; total {elapsed_time = }")
