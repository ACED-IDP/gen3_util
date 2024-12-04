import asyncio
import json
import logging
import pathlib
from datetime import datetime
import time
from urllib.parse import urlparse
from zipfile import ZipFile

from gen3.auth import Gen3Auth
from gen3.jobs import Gen3Jobs
import pytz

from gen3_tracker import Config
from gen3_tracker.common import Push, Commit
from gen3_tracker.gen3.indexd import write_indexd
from gen3_tracker.git import (
    calculate_hash,
    DVC,
    run_command,
    DVCMeta,
    DVCItem,
    modified_date,
)


def _validate_parameters(from_: str) -> pathlib.Path:

    assert (
        len(urlparse(from_).scheme) == 0
    ), f"{from_} appears to be an url. url to url cp not supported"

    return from_


def cp(
    config: Config,
    from_: str,
    project_id: str,
    ignore_state: bool,
    auth=None,
    user=None,
    object_name=None,
    bucket_name=None,
    metadata: dict = {},
):
    """Copy meta to bucket, used by etl_pod job"""
    from_ = _validate_parameters(str(from_))
    if not isinstance(from_, pathlib.Path):
        from_ = pathlib.Path(from_)

    assert auth, "auth is required"

    metadata = dict(
        {"submitter": None, "metadata_version": "0.0.1", "is_metadata": True} | metadata
    )
    if not metadata["submitter"]:
        if not user:
            user = auth.curl("/user/user").json()
        metadata["submitter"] = user["name"]

    program, project = project_id.split("-")

    assert bucket_name, f"could not find bucket for {program}"

    temp_dir = config.work_dir

    temp_dir = pathlib.Path(temp_dir)

    if not object_name:
        now = datetime.now().strftime("%Y%m%d-%H%M%S")
        object_name = f"_{project_id}-{now}_meta.zip"

    zipfile_path = temp_dir / object_name
    with ZipFile(zipfile_path, "w") as zip_object:
        for _ in from_.glob("*.ndjson"):
            zip_object.write(_)

    stat = zipfile_path.stat()
    md5_sum = calculate_hash("md5", zipfile_path)
    my_dvc = DVC(
        meta=DVCMeta(),
        outs=[
            DVCItem(
                path=object_name,
                md5=md5_sum,
                hash="md5",
                modified=modified_date(zipfile_path),
                size=stat.st_size,
            )
        ],
    )

    metadata = write_indexd(
        auth=auth,
        project_id=project_id,
        bucket_name=bucket_name,
        overwrite=False,
        restricted_project_id=None,
        dvc=my_dvc,
    )

    # document = file_client.upload_file_to_guid(guid=id_, file_name=object_name, bucket=bucket_name)
    # print(document, file=sys.stderr)

    run_command(
        f"gen3-client upload-single --bucket {bucket_name} --guid {my_dvc.object_id} --file {zipfile_path} --profile {config.gen3.profile}",
        no_capture=False,
    )

    return {
        "msg": f"Uploaded {zipfile_path} to {bucket_name}",
        "object_id": my_dvc.object_id,
        "object_name": object_name,
    }


def publish_commits(
    config: Config, wait: bool, auth: Gen3Auth, bucket_name: str, spinner=None
) -> dict:
    """Publish commits to the portal."""

    # TODO legacy fhir-import-export job: copies meta to bucket and triggers job,
    #  meta information is already in git REPO,
    #  we should consider changing the fhir_import_export job to use the git REPO

    user = auth.curl("/user/user").json()

    # copy meta to bucket
    upload_result = cp(
        config=config,
        from_="META",
        project_id=config.gen3.project_id,
        ignore_state=True,
        auth=auth,
        user=user,
        bucket_name=bucket_name,
    )

    object_id = upload_result["object_id"]

    push = Push(config=config)
    jobs_client = Gen3Jobs(auth_provider=auth)

    # create "legacy" commit object, read by fhir-import-export job
    push.commits.append(
        Commit(
            object_id=object_id,
            message="From g3t-git",
            meta_path=upload_result["object_name"],
            commit_id=object_id,
        )
    )
    args = {
        "push": push.model_dump(),
        "project_id": config.gen3.project_id,
        "method": "put",
    }

    # capture logging from gen3.jobs
    from cdislogging import get_logger  # noqa

    cdis_logging = get_logger("__name__")
    cdis_logging.setLevel(logging.WARN)

    if wait:
        # async_run_job_and_wait monkeypatched below
        _ = asyncio.run(
            jobs_client.async_run_job_and_wait(
                job_name="fhir_import_export", job_input=args, spinner=spinner
            )
        )
    else:
        _ = jobs_client.create_job("fhir_import_export", args)

    if not isinstance(_, dict):
        _ = {"output": _}
    if isinstance(_["output"], str):
        try:
            _["output"] = json.loads(_["output"])
        except json.JSONDecodeError:
            pass
    return _


# monkey patch for gen3.jobs.Gen3Jobs.async_run_job_and_wait
# make it less noisy and sleep less (max of 30 seconds)
async def async_run_job_and_wait(
    self, job_name, job_input, spinner=None, _ssl=None, **kwargs
):
    """
    Asynchronous function to create a job, wait for output, and return. Will
    sleep in a linear delay until the job is done, starting with 1 second.

    Args:
        _ssl (None, optional): whether or not to use ssl
        job_name (str): name for the job, can use globals in this file
        job_input (Dict): dictionary of input for the job

    Returns:
        Dict: Response from the endpoint
    """
    job_create_response = await self.async_create_job(job_name, job_input)
    status = {"status": "Running", "name": job_name}
    initial_sleep_time = 3
    sleep_time = initial_sleep_time
    max_sleep_time = 30
    while status.get("status") == "Running":
        if spinner:
            spinner.text = f"{status.get('name')} waiting {int(sleep_time)}"
        else:
            logging.debug(f"job still running, waiting for {sleep_time} seconds...")
        time.sleep(sleep_time)
        sleep_time *= 1.5
        if sleep_time > max_sleep_time:
            sleep_time = initial_sleep_time
        status = await self.async_get_status(job_create_response.get("uid"))
        if not spinner:
            logging.info(f"{status}")

    if not spinner:
        logging.info("Job is finished!")
    else:
        spinner.text = f"{status.get('name')} {status.get('status')}"

    if status.get("status") != "Completed":
        # write failed output to log file before raising exception
        response = await self.async_get_output(job_create_response.get("uid"))
        with open("logs/publish.log", "a") as f:
            log_msg = {"timestamp": datetime.now(pytz.UTC).isoformat()}
            log_msg.update(response)
            f.write(json.dumps(log_msg, separators=(",", ":")))
            f.write("\n")

        raise Exception(f"Job status not complete: {status.get('status')}")

    response = await self.async_get_output(job_create_response.get("uid"))
    return response


Gen3Jobs.async_run_job_and_wait = async_run_job_and_wait
