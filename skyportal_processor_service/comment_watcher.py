import argparse
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import traceback
from typing import Dict, List, Optional, Set, Tuple
import json

import requests
from config import get_required_env, load_env_file
from requests.auth import HTTPBasicAuth
import logging
logging.getLogger().setLevel(logging.DEBUG)

# TODO
# Do we need to continue where left off based on comments? Maybe just start listening from now() and not care about past?
# verify radec of Skyportal source is the one we need
# uploading files takes time -> maybe put into separate thread
# put group id everywhere
# docker-compose
# tests

header = {}
seen_comments: Dict[str, Set[str]] = dict()  # Track seen comments (by source + created at) to avoid duplication
# Memory-only, not persisted (restart re-derives everything from SkyPortal history anyway).

# source_id -> (job_id, ra, dec) queued while a job for that source is already in `pipeline`;
pending_rerun: Dict[str, Tuple[int, float, float]] = dict()

jobformat_version = 0.2

# WebDAV PROPFIND depth used to list a source's whole directory tree in one request.
# The real GRANDMA folder structure is source/telescope/[subfolder/]files (<=3 levels),
# but this server (SabreDAV) rejects "Depth: infinity" outright, so a generous fixed
# numeric depth is used instead; the server naturally stops at the actual tree depth.
OWNCLOUD_SCAN_DEPTH = 10


@dataclass
class FileJob:
    """
    State of a single file's StdWeb task within a rescan.
    """
    path: str = ""  # OwnCloud path of the file, relative to SAVE_PATH (e.g. "GRB-260708A/Les-Makes-T60/foo.fits")
    stdweb_id: int = -1
    # Stages: 3=create task, 4=upload, 5=start, 6=poll, 7=upload photometry to SkyPortal, 1000=done, 1001=error
    stage: int = 3
    error: str = ""

    def to_dict(self) -> dict:
        return {"path": self.path, "stdweb_id": self.stdweb_id, "stage": self.stage, "error": self.error}

    @staticmethod
    def from_dict(fields: dict) -> "FileJob":
        return FileJob(
            path=fields["path"],
            stdweb_id=fields.get("stdweb_id", -1),
            stage=fields.get("stage", 3),
            error=fields.get("error", ""),
        )


@dataclass
class RescanJob:
    """
    Data structure for saving the state of a source rescan job.
    """
    comment: str = ""
    source_id: str = ""
    job_id: int = 0  # Local for the source (id of the triggering comment)
    # Stages:
    #  see pipeline_tick
    #  1000 - done
    #  1001 - error
    stage: int = 0
    ra: float = 0
    dec: float = 0
    author: str = ""
    processed_files: List[str] = field(default_factory=list)  # successfully processed, cumulative across rescans
    failed_files: List[str] = field(default_factory=list)  # permanently failed, cumulative across rescans
    file_jobs: List[FileJob] = field(default_factory=list)  # this rescan's in-flight files
    version: float = jobformat_version

    # Dict is very inefficient, but back-compatible
    def serialize(self) -> str:
        return json.dumps({
            "comment": self.comment,
            "source_id": self.source_id,
            "job_id": self.job_id,
            "stage": self.stage,
            "ra": self.ra,
            "dec": self.dec,
            "author": self.author,
            "processed_files": self.processed_files,
            "failed_files": self.failed_files,
            "file_jobs": [f.to_dict() for f in self.file_jobs],
            "version": self.version,
            })

    @staticmethod
    def deserialize(string: str) -> "RescanJob":
        fields = json.loads(string)
        return RescanJob(
            comment=fields["comment"],
            source_id=fields["source_id"],
            job_id=fields["job_id"],
            stage=fields["stage"],
            ra=fields.get("ra", 0),
            dec=fields.get("dec", 0),
            author=fields.get("author", ""),
            processed_files=fields.get("processed_files", []),
            failed_files=fields.get("failed_files", []),
            file_jobs=[FileJob.from_dict(f) for f in fields.get("file_jobs", [])],
            version=fields.get("version", 0.0),
        )
pipeline: List[RescanJob] = []


def str_to_varsel_str(message: str) -> str:
    """
    Encode a string as a string of variation selectors (invisible unicode characters that take up 0 pixels).
    Each two characters in the output string are 4 bytes in total and they encode 1 byte of the input string.
    """
    return "".join(map(lambda x: chr(0xfe00+x%16)+chr(0xfe00+x//16), message.encode("utf-8")))


def varsel_str_to_str(invisible_chars: str) -> str:
    """
    Decode a string from a string of variation selectors (invisible unicode characters that take up 0 pixels).
    Each two characters in the input string are 4 bytes in total and they encode 1 byte of the output string. Therefore the length of the input must be even.
    """
    bytelist = []
    for i in range(0, len(invisible_chars)-1, 2):
        bytelist.append(
            (ord(invisible_chars[i  ])-0xfe00) + 
            (ord(invisible_chars[i+1])-0xfe00) * 16
        )
    return bytes(bytelist).decode("utf-8")


def get_new_comments(
    start_time: datetime, last_iteration: Optional[datetime]
) -> Tuple[List[RescanJob], datetime]:
    """
    One API call: ask SkyPortal which sources have comment activity since last
    check.

    A comment containing SKYPORTAL_COMMAND immediately followed by a non-space
    suffix (e.g. "...help", "...dump 5", a typo'd subcommand) is an explicit
    operator command: it always gets its own job immediately, bypassing both
    same-tick coalescing and the already-running-job queue below, since it's a
    direct request for a synchronous reply (stage 1 resolves it in one pass) and
    users should never have to wait behind an unrelated rescan to get one.

    Every other (bare or content-free) comment goes through coalescing: at most
    one job stub is created per source per call -- if a source gets several such
    comments in one tick, only the latest *non-bot* comment is used (bot replies
    always outrank the human comment that spawned them by id, so they'd otherwise
    win and silently swallow the real trigger); if a job for that source is
    already running (in `pipeline`), no job is created at all -- the latest
    comment id is queued in `pending_rerun` and fired the instant the active job
    finishes (see pipeline_tick_all), not on the next poll.

    Bot exclusion and explicit-command detection here are purely structural
    (scheduling/routing only); actual command execution and files-restore are
    entirely pipeline_tick's stage 1 ("get the comments").

    Updates the last_iteration timestamp before making the API call to ensure
    no comments are missed between polling cycles. Only considers comments that
    haven't been seen before (tracked in the global seen_comments set).

    Args:
        start_time: Initial start time for fetching sources
        last_iteration: Timestamp of last successful fetch, or None for first run

    Returns:
        Tuple of (list of job stubs, updated last iteration timestamp)
    """
    since = last_iteration if last_iteration else start_time

    # Update last iteration before fetching new sources to avoid missing any sources
    last_iteration = datetime.utcnow().replace(tzinfo=timezone.utc)

    response = requests.get(
        f"{INSTANCE_URL}/api/sources",
        params={
            "includeComments": True,
            "commentsFilterAfter": since.isoformat(),
            #"group_ids": SKYPORTAL_GROUP_IDS_FILTER,
        },
        headers=header,
    )
    response.raise_for_status()
    logger.debug(f"Respose time: {response.elapsed.total_seconds() * 1000}ms")

    sources = response.json()["data"]["sources"]
    explicit_command_jobs = []
    latest_per_source = {}  # source_id -> (comment, source), keeping only the highest comment id
    for source in sources:
        if source["id"] not in seen_comments:
            seen_comments[source["id"]] = set()
        seen_comments_for_source = seen_comments[source["id"]]

        for comment in source["comments"]:
            if comment.get("bot") is not False:  # Skip bots
                continue
            if comment["created_at"] in seen_comments_for_source:
                continue
            # Filter by created at, so that modifying the comment doesn't do anything
            seen_comments_for_source.add(comment["created_at"])

            command_suffix = comment["text"].split(SKYPORTAL_COMMAND)[1] if SKYPORTAL_COMMAND in comment["text"] else ""
            if command_suffix.split(" ")[0]:
                explicit_command_jobs.append(RescanJob(
                    source_id=source["id"],
                    job_id=comment["id"],
                    stage=1,
                    ra=source["ra"],
                    dec=source["dec"],
                ))
                continue

            existing = latest_per_source.get(source["id"])
            if existing is None or comment["id"] > existing[0]["id"]:
                latest_per_source[source["id"]] = (comment, source)

    new_jobs = list(explicit_command_jobs)
    active_sources = {j.source_id for j in pipeline}
    for source_id, (comment, source) in latest_per_source.items():
        if source_id in active_sources:
            # Schedule a rescan after the current job finishes
            pending_rerun[source_id] = (comment["id"], source["ra"], source["dec"])
            continue
        new_jobs.append(RescanJob(
            source_id=source_id,
            job_id=comment["id"],
            stage=1,
            ra=source["ra"],
            dec=source["dec"],
        ))
    return new_jobs, last_iteration


def comment_back(job: RescanJob, message: str) -> None:
    """
    Post a comment to SkyPortal to report the progress of the job.
    """
    response = requests.post(
        f"{INSTANCE_URL}/api/sources/{job.source_id}/comments",
        json={
            "text": f"№{job.job_id}:{str_to_varsel_str(job.serialize())} {message} @{job.author}",
            #"group_ids": SKYPORTAL_GROUP_IDS_FILTER,
        },
        headers=header,
    )
    response.raise_for_status()


def job_from_comment(comment: str) -> Optional[RescanJob]:
    """
    Parse the hidden serialized job from a bot comment, if any.
    """
    text1 = comment.split(":")
    if len(text1) < 2:
        return None
    text2 = text1[1].split(" ")[0]
    if not text2.strip():
        return None
    text = varsel_str_to_str(text2)
    try:
        return RescanJob.deserialize(text)
    except Exception:
        traceback.print_exc()
        return None


def find_files_in_comments(comments: List[dict], source_id: str) -> Tuple[List[str], List[str]]:
    """
    Scan an already-fetched comment list (unbounded by the polling window) and
    accumulate the processed/failed file ledger across *every* bot reply found
    for this source, from every job -- union, not "trust the most recent one".

    This is deliberately robust against two ways a single latest-wins approach
    breaks: (a) diagnostic-only jobs (help/dump/show/unrecognized/bare-disallow)
    reach a terminal stage without ever touching processed_files/failed_files --
    if one of those happened to be the most recent reply, trusting it alone would
    silently wipe out a real ledger; with a union they just contribute nothing
    (empty set). (b) explicit-command jobs now run concurrently alongside an
    active rescan for the same source, so completions for one source can
    interleave -- a union combines them instead of letting one overwrite another.
    Real per-file results are always safe to include regardless of a job's own
    final stage (1000 success or 1001 mid-run crash): whatever made it into a
    job's processed_files/failed_files reflects a StdWeb outcome that actually
    happened and must never be silently forgotten. If the same file ever shows
    up as both processed and failed across different jobs, success wins -- it's
    removed from the failed set before returning.

    Returns ([], []) if this source has never had a rescan reply. Pure function,
    no I/O -- pipeline_tick's stage 1 fetches the comments once and reuses them here.
    """
    processed = set()
    failed = set()
    for comment in comments:
        if not comment["bot"] or not comment["text"] or comment["text"][0] != "№":
            continue
        job = job_from_comment(comment["text"])
        if job is None or job.source_id != source_id:
            continue
        processed.update(job.processed_files)
        failed.update(job.failed_files)
    failed -= processed
    return list(processed), list(failed)


def owncloud_exists(path: str) -> bool:
    """
    Check if a file or folder exists and is accessible in OwnCloud.
    """
    check_url = f"{BASE_URL}/remote.php/dav/files/{OWNCLOUD_USER_ID}/{SAVE_PATH}/{path}"
    response = requests.request(
        "PROPFIND", check_url, auth=HTTPBasicAuth(OWNCLOUD_USERNAME, OWNCLOUD_TOKEN), headers={"Depth":"0"}
    )
    return response.status_code in [200, 207]


def owncloud_ls(path: str, depth: int = 1) -> Tuple[List[str], List[str]]:
    """
    List all files and folders in an OwnCloud directory.

    Depth indicates whether to also list contents of subfolders: 1 is just the requested folder, 2 is including its subfolder, etc.

    Returns a list of full paths and a list of relative paths
    """
    check_url = f"{BASE_URL}/remote.php/dav/files/{OWNCLOUD_USER_ID}/{SAVE_PATH}/{path}"
    response = requests.request(
        "PROPFIND", check_url, auth=HTTPBasicAuth(OWNCLOUD_USERNAME, OWNCLOUD_TOKEN), headers={"Depth":str(depth)}
    )

    if response.status_code != 207:
        return [], []
    
    result = []
    result_short = []
    i1 = response.text.find("<d:href>")
    while i1 != -1:
        i2 = response.text.find("</d:href>", i1)
        if i2 == -1:
            return result, result_short
        file = response.text[i1+len("<d:href>"):i2]
        result.append(file)
        result_short.append(file[len(check_url)-len(BASE_URL):])
        i1 = response.text.find("<d:href>", i2)
    return result, result_short


def stdweb_make_task(source_id: str, job_id: int, file_path: str, local_file: Optional[str] = None) -> int:
    """
    Create a new task on StdWeb for a single file.

    If `local_file` is given (StdWeb's DATA_PATH-relative path to the same file,
    reachable because OwnCloud is mounted read-only under DATA_PATH), StdWeb
    copies the file itself from that mount at creation time -- no separate
    upload call needed, no bytes transloaded through this process at all.
    Otherwise the caller must still populate the task via
    transfer_file_owncloud_to_stdweb.

    Returns task id of the new task.
    """
    data = {"original_name": f"{source_id}/{file_path.split('/')[-1]} : {job_id}"}
    if local_file:
        data["local_file"] = local_file
    response = requests.request(
        "POST", f"{STDWEB_URL}/api/tasks/",
        headers={"Authorization": f"Token {STDWEB_TOKEN}"},
        data=data
    )
    response.raise_for_status()
    return response.json()["id"]


def stdweb_start_task(stdweb_id: int, ra: float, dec: float) -> None:
    """
    Set config of the task on StdWeb and start it (single-file forced photometry, no stacking).
    """
    response = requests.request(
        "POST", f"{STDWEB_URL}/api/tasks/{stdweb_id}/process/",
        headers={"Authorization": f"Token {STDWEB_TOKEN}"},
        json={
            "steps": ["inspect", "photometry"],
            "config": {
                "target": f"{ra:.15f},{dec:.15f}",
            }
        }
    )
    response.raise_for_status()


def stdweb_task_status(stdweb_id: int) -> str:
    """
    Queries StdWeb task status.
    Returns task status as string.
    """
    response = requests.request(
        "GET", f"{STDWEB_URL}/api/tasks/{stdweb_id}/", 
        headers={"Authorization": f"Token {STDWEB_TOKEN}"}
    )
    response.raise_for_status()
    return response.json()["state"]


def stdweb_upload_photometry(stdweb_id: int, source_id: str, instrument_name: str, ra: float, dec: float) -> None:
    """
    Ask StdWeb to push this task's forced-photometry result to a SkyPortal source
    (StdWeb does the actual SkyPortal PUT itself, using its own SkyPortal token --
    see stdweb/api/views.py:task_skyportal_upload). Raises RuntimeError with the
    server's error detail on failure.
    """
    response = requests.request(
        "POST", f"{STDWEB_URL}/api/tasks/{stdweb_id}/skyportal/",
        headers={"Authorization": f"Token {STDWEB_TOKEN}"},
        json={"source_id": source_id, "instrument_name": instrument_name, "ra": ra, "dec": dec},
    )
    if not response.ok:
        try:
            detail = response.json().get("error", response.text)
        except ValueError:
            detail = response.text
        raise RuntimeError(f"StdWeb error {response.status_code}: {detail}")

def transfer_file_owncloud_to_stdweb(owncloud_path: str, stdweb_task_id: int, stdweb_filename: Optional[str] = None) -> None:
    download_url = f"{BASE_URL}/remote.php/dav/files/{OWNCLOUD_USER_ID}/{SAVE_PATH}/{owncloud_path}"
    response = requests.request(
        "GET", download_url, auth=HTTPBasicAuth(OWNCLOUD_USERNAME, OWNCLOUD_TOKEN)
    )
    response.raise_for_status()
    if stdweb_filename is None:
        stdweb_filename = owncloud_path.split("/")[-1]
    response2 = requests.request(
        "POST", f"{STDWEB_URL}/api/tasks/{stdweb_task_id}/files/{stdweb_filename}",
        headers={"Authorization": f"Token {STDWEB_TOKEN}"},
        files={"file": (stdweb_filename, response.content, "application/fits")}
    )
    response2.raise_for_status()


def pipeline_tick(job: RescanJob) -> bool:
    """
    Main function that runs for all jobs. Broken up into pieces to allow waiting for something to happen asynchronously (and continuing from checkpoints even after restarting python).

    Every comment on a source triggers a job stub (get_new_comments does zero
    content inspection). The whole flow below -- fetching/validating the actual
    trigger comment, restoring the files, listing OwnCloud, creating tasks -- is
    silent unless it finds something worth reporting: new files, a failure, or
    the triggering comment itself mentions "owncloud"/"upload" (in which case a
    "nothing new" reply is sent too, so a human who says they uploaded something
    gets confirmation either way). Explicit "{SKYPORTAL_COMMAND}help/dump/show"
    commands always reply.

    Flow (labelled by job.stage):
    1. Get the comments (one API call): find the trigger comment, bail out
       silently if it's bot-authored (this is the only bot check anywhere --
       get_new_comments doesn't inspect comment content at all) or was deleted,
       parse an explicit command if the comment mentions one, and restore the
       processed/failed files from the same fetch
    2. List the entire OwnCloud source directory (recursively) and diff it against the files
    3. Create a StdWeb task per unprocessed file and drive each through
       create/upload/start/poll, then upload the result to SkyPortal (each file's
       telescope, taken from its OwnCloud path, must be mapped to a SkyPortal
       instrument name in TELESCOPE_INSTRUMENT_MAP or the file fails outright)
    1000. Done
    1001 = Error caught

    Returns whether the job has finished
    """
    try:
        if job.stage <= 1:  # Immediately after seeing the comment
            response = requests.get(
                f"{INSTANCE_URL}/api/sources/{job.source_id}/comments",
                headers=header,
            )
            response.raise_for_status()
            comments = response.json()["data"]

            trigger = next((c for c in comments if c["id"] == job.job_id), None)
            if trigger is None or trigger.get("bot") is not False:
                # Comment vanished, or was bot-authored (missing/null bot flag treated
                # conservatively as bot too). Never legitimately reachable today since
                # get_new_comments doesn't filter by bot at all -- this check IS the
                # bot filter now, guarding against a self-triggering reply loop.
                job.stage = 1000
                return True
            job.comment = trigger["text"]
            job.author = trigger["author_id"]

            if SKYPORTAL_COMMAND in job.comment:
                command = job.comment.split(SKYPORTAL_COMMAND)[1]
                argv = command.split(" ")
                help_message = f"Usage: any comment triggers a rescan of this source's OwnCloud directory. Feedback is silent unless new files are found (or fail), or your comment mentions 'owncloud'/'upload'.\nExplicit commands (no space before the keyword):\n{SKYPORTAL_COMMAND}help\n{SKYPORTAL_COMMAND}dump [limit]\n{SKYPORTAL_COMMAND}show <invisible text>."
                if argv[0]:
                    # This is a special command
                    if argv[0] == "help":
                        job.stage = 1000
                        comment_back(job, help_message)
                        return True
                    if argv[0] == "dump":
                        job.stage = 1000
                        count = len(pipeline)
                        if len(argv) > 1 and argv[1].isdigit():
                            count = min(len(pipeline), int(argv[1]))
                        comment_back(job, "\n"+"\n".join(map(lambda j: j.serialize(), pipeline[0:count])))
                        return True
                    if argv[0] == "show":
                        job.stage = 1000
                        count = 100
                        if len(argv) > 1:
                            if argv[1].isdigit():
                                count = int(argv[1])
                            else:
                                decoded = varsel_str_to_str("".join(filter(lambda x: 0xfe00 <= ord(x) <= 0xfe0f, command)))
                                comment_back(job, decoded)
                                return True
                        comment_back(job, "TODO, for now you have to give №x: .")
                        return True
                    job.stage = 1001
                    comment_back(job, f"Unrecognized command. {help_message}")
                    return True
                if job.comment.strip() == SKYPORTAL_COMMAND:
                    # Bare "@rescan" and nothing else is disallowed: any comment already
                    # triggers a rescan, and repeating this exact minimal text hits
                    # SkyPortal's same-author dedup, silently swallowing the retrigger.
                    # "@rescan" embedded in a larger, naturally-varying comment is fine.
                    job.stage = 1001
                    comment_back(
                        job,
                        f'Don\'t post a bare "{SKYPORTAL_COMMAND}" \u2014 any comment on this source '
                        f"already triggers a check, and identical repeated text is silently dropped "
                        f"by SkyPortal. {help_message}",
                    )
                    return True
                # "@rescan" mentioned within other text: proceed to a silent rescan.

            job.processed_files, job.failed_files = find_files_in_comments(comments, job.source_id)
            job.stage = 2

        if job.stage <= 2:
            if owncloud_exists(job.source_id):
                _, files_short = owncloud_ls(job.source_id, depth=OWNCLOUD_SCAN_DEPTH)
                fits_files = [job.source_id + f for f in files_short if f.endswith(".fits") or f.endswith(".fit")]
            else:
                # No OwnCloud folder for this source (expected/common now that every
                # comment triggers a rescan, not just deliberate ones) -> nothing to do.
                fits_files = []
            known = set(job.processed_files) | set(job.failed_files)
            pending = [f for f in fits_files if f not in known]
            job.file_jobs = [FileJob(path=f) for f in pending]
            job.stage = 3
            if pending:
                comment_back(job, f"{len(pending)} new file(s) found ({len(known)} already processed). Creating tasks.")

        if job.stage == 3:
            newly_done = []
            newly_failed = []
            for file_job in job.file_jobs:
                if file_job.stage >= 1000:
                    continue
                try:
                    if file_job.stage <= 3:
                        if STDWEB_SAVE_PATH:
                            # OwnCloud mounted read-only under StdWeb's DATA_PATH:
                            # StdWeb copies the file itself, no transload stage needed.
                            file_job.stdweb_id = stdweb_make_task(
                                job.source_id, job.job_id, file_job.path,
                                local_file=f"{STDWEB_SAVE_PATH}/{file_job.path}",
                            )
                            file_job.stage = 5
                        else:
                            file_job.stdweb_id = stdweb_make_task(job.source_id, job.job_id, file_job.path)
                            file_job.stage = 4
                    if file_job.stage <= 4:
                        transfer_file_owncloud_to_stdweb(file_job.path, file_job.stdweb_id, "image.fits")
                        file_job.stage = 5
                    if file_job.stage <= 5:
                        stdweb_start_task(file_job.stdweb_id, job.ra, job.dec)
                        file_job.stage = 6
                    if file_job.stage <= 6:
                        status = stdweb_task_status(file_job.stdweb_id)
                        if "failed" in status:
                            file_job.stage = 1001
                            file_job.error = status
                            job.failed_files.append(file_job.path)
                            newly_failed.append(file_job)
                        elif status == "photometry_done":
                            file_job.stage = 7
                    if file_job.stage == 7:
                        task_link = f"{STDWEB_URL}/tasks/{file_job.stdweb_id}"
                        # file_job.path is "<source_id>/<telescope>/[subfolder/]<filename>".
                        telescope = file_job.path.split("/")[1] if file_job.path.count("/") >= 2 else None
                        instrument_name = TELESCOPE_INSTRUMENT_MAP.get(telescope) if telescope else None
                        if instrument_name is None:
                            raise RuntimeError(
                                f"Unmapped telescope {telescope!r} -- add it to TELESCOPE_INSTRUMENT_MAP ({task_link})"
                            )
                        try:
                            stdweb_upload_photometry(file_job.stdweb_id, job.source_id, instrument_name, job.ra, job.dec)
                        except Exception as e:
                            raise RuntimeError(f"SkyPortal upload failed: {e} ({task_link})")
                        file_job.stage = 1000
                        job.processed_files.append(file_job.path)
                        newly_done.append(file_job)
                except Exception as e:
                    file_job.stage = 1001
                    file_job.error = str(e)
                    job.failed_files.append(file_job.path)
                    newly_failed.append(file_job)

            if newly_done or newly_failed:
                parts = []
                if newly_done:
                    parts.append(f"{len(newly_done)} done: {', '.join(f.path.split('/')[-1] for f in newly_done)}")
                if newly_failed:
                    parts.append(f"{len(newly_failed)} failed: {', '.join(f.path.split('/')[-1] for f in newly_failed)}")
                comment_back(job, "; ".join(parts) + ".")

            if all(file_job.stage >= 1000 for file_job in job.file_jobs):
                job.stage = 1000
            else:
                return False
    except Exception as e:
        job.stage = 1001
        traceback.print_exc()
        logger.error(f"Error while processing job #{job.job_id} for source {job.source_id}")
        # Hopefully this will not error
        comment_back(job, str(e))
        return True

    new_done = sum(1 for f in job.file_jobs if f.stage == 1000)
    new_failed = sum(1 for f in job.file_jobs if f.stage == 1001)
    keywords_mentioned = any(k in job.comment.lower() for k in ("owncloud", "upload"))
    if new_done or new_failed or keywords_mentioned:
        comment_back(
            job,
            f"Rescan complete: {new_done} new file(s) processed and uploaded to SkyPortal, {new_failed} new file(s) failed. "
            f"Total: {len(job.processed_files)} processed, {len(job.failed_files)} failed.",
        )
    return True

def pipeline_tick_all() -> None:
    """
    Run all tasks in the pipeline and remove the ones that have finished. When a
    job finishes, if another comment arrived for its source while it was running
    (queued in pending_rerun by get_new_comments), start that rerun immediately
    rather than waiting for the next poll.
    """
    i = 0
    while i < len(pipeline):
        job = pipeline[i]
        if pipeline_tick(job):
            pipeline.pop(i)
            queued = pending_rerun.pop(job.source_id, None)
            if queued is not None:
                job_id, ra, dec = queued
                pipeline.append(RescanJob(source_id=job.source_id, job_id=job_id, stage=1, ra=ra, dec=dec))
        else:
            i += 1

def main_loop(start_time: datetime) -> None:
    """
    Main monitoring loop that watches for new comments, creates and runs jobs.

    Continuously polls SkyPortal and runs active pipeline jobs.

    Args:
        start_time: Datetime to start monitoring from
    """
    # try:
    #     create_base_folder_on_owncloud()
    # except Exception as e:
    #     logger.error(f"Error during base folder creation: {e}")

    logger.info("Listening for new sources...\n")

    last_iteration = None
    while True:
        try:
            new_comments, last_iteration = get_new_comments(start_time, last_iteration)
            for job in new_comments:
                source_id = job.source_id
                logger.info(f"New command detected for {source_id}: {job.comment}")
                pipeline.append(job)
            if new_comments:
                logger.info("Listening for new comments...")
            
            pipeline_tick_all()
        except Exception as e:
            logger.error(f"Error: {e}")
        time.sleep(POLL_INTERVAL)


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        Parsed command line arguments namespace
    """
    parser = argparse.ArgumentParser(
        description=(
            "SkyPortal source watcher\nAuto-create folders in ownCloud for new sources.\n\n"
            "Example:\n"
            "  python source_watcher.py --env-file .env.local\n"
            "  python source_watcher.py --start-time '2025-05-15T00:00:00Z'\n\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--env-file",
        type=str,
        default=".env",
        help="Path to .env configuration file (default: .env)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Parse arguments and load environment
    args = parse_args()
    load_env_file(args.env_file)

    # Setup logger
    # slack_token = os.getenv("SLACK_BOT_TOKEN")
    # service_name = os.getenv("SLACK_SERVICE_NAME", "owncloud-folder-service")
    # channel = "#" + service_name
    logger = logging #setup_logger(service_name, slack_token, channel)

    try:
        BASE_URL = os.getenv(
            "OWNCLOUD_BASE_URL",
            "https://grandma-owncloud.lal.in2p3.fr",
        )
        OWNCLOUD_USERNAME = get_required_env("OWNCLOUD_USERNAME")
        OWNCLOUD_TOKEN = get_required_env("OWNCLOUD_TOKEN")
        OWNCLOUD_USER_ID = get_required_env("OWNCLOUD_USER_ID")
        INSTANCE_URL = os.getenv(
            "SKYPORTAL_URL", "https://skyportal-icare.ijclab.in2p3.fr"
        )
        SKYPORTAL_TOKEN = get_required_env("SKYPORTAL_TOKEN")
        STDWEB_URL = os.getenv(
            "STDWEB_URL", "https://grandma-stdpipe.ijclab.in2p3.fr"
        )
        STDWEB_TOKEN = get_required_env("STDWEB_TOKEN")


        SAVE_PATH = os.getenv("SAVE_PATH", "Candidates/Skyportal")
        # Empty by default: only set this once OwnCloud is actually mounted read-only
        # under StdWeb's DATA_PATH (see stdweb_make_task's local_file docs) -- setting
        # it prematurely points StdWeb at a path that doesn't exist there yet.
        STDWEB_SAVE_PATH = os.getenv("STDWEB_SAVE_PATH", "")
        SOURCE_TAG = os.getenv("SOURCE_TAG", "")
        POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
        SKYPORTAL_GROUP_IDS_FILTER = [
            int(x.strip()) for x in os.getenv("GROUP_IDS", "").split(",") if x.strip()
        ]
        SKYPORTAL_COMMAND=os.getenv("SKYPORTAL_COMMAND", "@rescan")
        # OwnCloud telescope folder name -> SkyPortal instrument name (exact match),
        # e.g. {"TAROT-TCA": "TAROT Calern imager"}. Required and exhaustive: a file
        # under a telescope folder missing from this map fails stage 7 outright
        # rather than falling back to a generic/default instrument.
        TELESCOPE_INSTRUMENT_MAP = json.loads(get_required_env("TELESCOPE_INSTRUMENT_MAP"))

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error(
            "Required variables: OWNCLOUD_USERNAME, OWNCLOUD_TOKEN, OWNCLOUD_USER_ID, SKYPORTAL_TOKEN, STDWEB_TOKEN, TELESCOPE_INSTRUMENT_MAP"
        )
        exit(1)
    header = {"Authorization": f"token {SKYPORTAL_TOKEN}"}

    # Parse start time from environment variable
    start_time_str = os.getenv("START_TIME")
    if start_time_str:
        try:
            start_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
        except ValueError:
            logger.error(
                "Invalid START_TIME format. Use ISO format, e.g. '2025-10-20T00:00:00Z'"
            )
            exit(1)
    else:
        start_time = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=1)
    main_loop(start_time)
