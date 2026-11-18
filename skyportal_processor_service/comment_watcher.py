import argparse
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import traceback
from typing import Dict, List, Optional, Set, Tuple
import json
import re
from urllib.parse import urlparse

import requests
from config import get_required_env, load_env_file
from requests.auth import HTTPBasicAuth
import logging

# A dedicated logger -- not `logging` the module itself (that was the old
# `logger = logging` assignment in __main__: works by accident, since the
# module exposes same-named forwarding functions, but isn't a real Logger).
# Third-party libraries (requests, urllib3) are deliberately left at
# logging's default WARNING level: only this service's own messages follow
# LOG_LEVEL (applied once the configured .env is loaded -- see `__main__`)
# -- the old `logging.getLogger().setLevel(logging.DEBUG)` set the *root*
# logger unconditionally at import time, which also forced every
# third-party DEBUG message (e.g. one urllib3 line per HTTP request) on
# any importer, with no way to opt out.
logging.basicConfig(level=logging.WARNING, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("comment_watcher")
logger.setLevel(logging.INFO)

# TODO
# verify radec of Skyportal source is the one we need
# uploading files takes time -> maybe put into separate thread
# put group id everywhere
# docker-compose
# tests

header = {}
seen_comments: Dict[str, Set[str]] = dict()  # Track seen comments (by source + created at) to avoid duplication
# Memory-only, not persisted (restart re-derives everything from SkyPortal history anyway).

# source_id -> (job_id, ra, dec, callback_url) queued while a job for that
# source is already in `pipeline`; callback_url is "" for a comment-
# triggered rerun (see get_new_comments/pipeline_tick_all).
pending_rerun: Dict[str, Tuple[int, float, float, str]] = dict()

jobformat_version = 0.2

# Every outbound call below blocks the single-threaded main_loop (one poll
# tick processes every job in `pipeline` sequentially) -- without an
# explicit timeout, a stalled/unresponsive remote (including an
# attacker-controlled webhook callback_url, see _is_valid_callback_url)
# would hang this entire service indefinitely. File transfer (FITS bytes
# between OwnCloud and StdWeb) gets a longer allowance than ordinary JSON
# API calls.
REQUEST_TIMEOUT = 30
FILE_TRANSFER_TIMEOUT = 300

# WebDAV PROPFIND depth used to list a source's whole directory tree in one request.
# The real GRANDMA folder structure is source/telescope/[subfolder/]files (<=3 levels),
# but this server (SabreDAV) rejects "Depth: infinity" outright, so a generous fixed
# numeric depth is used instead; the server naturally stops at the actual tree depth.
OWNCLOUD_SCAN_DEPTH = 10

# Analysis Service that stores each source's processed/failed file ledger
# (replaces the old mechanism of embedding job state as hidden unicode
# variation selectors appended to every bot reply comment and
# decoding/unioning them back on restart). Always upload_only=True --
# SkyPortal's analysis_upload endpoint (what save_ledger posts to) rejects
# anything else, so this service can't double as the webhook trigger below;
# see RESCAN_TRIGGER_SERVICE_NAME for that. Created once per SkyPortal
# instance -- see get_or_create_rescan_service.
RESCAN_SERVICE_NAME = "grandma_rescan"
RESCAN_SERVICE_ID: int = -1  # resolved once at startup

# Separate Analysis Service for SkyPortal's "Run Analysis" button (a webhook
# trigger, same effect as any comment) -- only created/managed when
# WEBHOOK_URL is configured; see get_or_create_trigger_service. Its own
# ObjAnalysis entries are just SkyPortal's UI feedback for that button; the
# actual ledger is always written to RESCAN_SERVICE_ID by save_ledger,
# regardless of which path triggered the run (see pipeline_tick).
RESCAN_TRIGGER_SERVICE_NAME = "grandma_rescan_trigger"
RESCAN_TRIGGER_SERVICE_ID: int = -1  # resolved once at startup if WEBHOOK_URL is set

# Webhook-triggered RescanJobs land here (from WebhookHandler.do_POST, which
# runs on the HTTP server's own thread) and are drained into `pipeline` at the
# top of each main_loop iteration (the only thread that mutates `pipeline`).
trigger_queue: "queue.Queue[RescanJob]" = queue.Queue()


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
    # Consecutive transient (network/5xx) failures retried from this
    # file's current checkpoint -- see pipeline_tick's MAX_FILE_RETRIES.
    # Not incremented for a permanent failure (those go straight to 1001).
    retry_count: int = 0

    def to_dict(self) -> dict:
        return {"path": self.path, "stdweb_id": self.stdweb_id, "stage": self.stage, "error": self.error, "retry_count": self.retry_count}


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
    # Set only for a job triggered via SkyPortal's "Run Analysis" webhook
    # (see WebhookHandler) rather than a comment -- where to POST the result
    # when the job finishes. "" for comment-triggered jobs.
    callback_url: str = ""
    processed_files: List[str] = field(default_factory=list)  # successfully processed, cumulative across rescans
    failed_files: List[str] = field(default_factory=list)  # permanently failed, cumulative across rescans
    file_jobs: List[FileJob] = field(default_factory=list)  # this rescan's in-flight files
    # path -> {"stdweb_id": int, "status": "processed"|"failed", "error": str}, cumulative
    # across rescans; carried over from the ledger on restore (see restore_ledger),
    # grows as file_jobs complete. Saved back to the ledger alongside
    # processed_files/failed_files -- see save_ledger.
    file_details: Dict[str, dict] = field(default_factory=dict)
    version: float = jobformat_version
    # FileJob objects (newly completed/failed, this run) not yet durably
    # recorded via save_ledger -- see pipeline_tick's stage 3. Retried every
    # tick until save_ledger actually succeeds, so a transient SkyPortal
    # hiccup while saving can't silently lose a file's outcome and cause it
    # to be reprocessed (duplicate StdWeb task, duplicate photometry
    # upload) on a later rescan. Not part of serialize(): same FileJob
    # objects already shown there via file_jobs.
    unsaved_results: List[FileJob] = field(default_factory=list)

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
            "file_details": self.file_details,
            "version": self.version,
            })
pipeline: List[RescanJob] = []


def _get_or_create_service(name: str, description: str, desired: dict) -> int:
    """
    Find the named Analysis Service, or create it if this is the first run
    against this SkyPortal instance; update it if `desired` (including
    display_name) no longer matches how it's currently configured there.
    Shared by get_or_create_rescan_service and get_or_create_trigger_service.

    Finding it only needs an ordinary token (GET is unrestricted); creating
    or updating it needs the "Manage Analysis Services" ACL, which
    SKYPORTAL_TOKEN won't have in most production setups (it only needs "Run
    Analyses" for everything else this service does). If it's missing and
    SKYPORTAL_TOKEN can't create it, fail loudly with the exact payload an
    admin needs to POST once. If it already exists but can't be updated,
    warn and keep using it as currently configured there -- this only matters
    if WEBHOOK_URL was just added or removed.
    """
    response = requests.get(f"{INSTANCE_URL}/api/analysis_service", headers=header, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    for service in response.json()["data"]:
        if service["name"] == name:
            if any(service.get(k) != v for k, v in desired.items()):
                patch = requests.patch(
                    f"{INSTANCE_URL}/api/analysis_service/{service['id']}", headers=header, json=desired,
                    timeout=REQUEST_TIMEOUT,
                )
                if patch.status_code != 200:
                    logger.warning(
                        f"Could not update {name!r} to match the current WEBHOOK_URL "
                        f"config (needs \"Manage Analysis Services\"); leaving it as currently configured "
                        f"on SkyPortal. Server response: {patch.status_code} {patch.text}"
                    )
            return service["id"]

    create_payload = {
        "name": name,
        "description": description,
        "authentication_type": "none",
        "analysis_type": "meta_analysis",
        "input_data_types": [],
        "timeout": 7200,
        **desired,
    }
    if SKYPORTAL_GROUP_IDS_FILTER:
        create_payload["group_ids"] = SKYPORTAL_GROUP_IDS_FILTER
    response = requests.post(f"{INSTANCE_URL}/api/analysis_service", headers=header, json=create_payload, timeout=REQUEST_TIMEOUT)
    if response.status_code != 200:
        raise RuntimeError(
            f"Analysis service {name!r} doesn't exist yet and SKYPORTAL_TOKEN can't "
            f"create it (needs the \"Manage Analysis Services\" ACL -- ordinary \"Run Analyses\" isn't "
            f"enough). Ask a SkyPortal admin to POST this once to {INSTANCE_URL}/api/analysis_service "
            f"(with their own, more privileged token), then restart this service:\n"
            f"{json.dumps(create_payload, indent=2)}\n"
            f"Server response: {response.status_code} {response.text}"
        )
    return response.json()["data"]["id"]


def get_or_create_rescan_service() -> int:
    """
    Find or create RESCAN_SERVICE_NAME: the permanent, always upload_only
    ledger-storage service save_ledger/restore_ledger/prune_ledger_entries
    use, regardless of which path (comment or webhook) triggered a run.
    Called once at startup, unconditionally.
    """
    return _get_or_create_service(
        RESCAN_SERVICE_NAME,
        (
            "Stores each source's processed/failed file ledger for the GRANDMA "
            "rescan pipeline (any comment on a source, or the separate "
            '"Rescan OwnCloud" trigger service if configured, triggers a run '
            "that writes here). upload_only: not meant to be run directly."
        ),
        desired={
            # Same display_name as the trigger service (see
            # get_or_create_trigger_service) -- distinguished internally by
            # the unique `name` field (analysis_services.name has a DB
            # unique constraint, display_name does not), not by what's shown
            # in the UI. This one never appears on the "Run Analysis"
            # dropdown (display_on_resource_dropdown: False), so the two
            # sharing a label isn't ambiguous in practice.
            "display_name": "Rescan OwnCloud",
            "upload_only": True,
            "url": f"{INSTANCE_URL}/not-a-real-analysis-endpoint",
            "display_on_resource_dropdown": False,
        },
    )


def get_or_create_trigger_service() -> int:
    """
    Find or create RESCAN_TRIGGER_SERVICE_NAME: the "Run Analysis" webhook
    entry point, only called when WEBHOOK_URL is configured. Deliberately a
    separate service from RESCAN_SERVICE_NAME -- SkyPortal's analysis_upload
    endpoint (what save_ledger posts to) only works against an upload_only
    service, so one service can't be both the ledger store and this
    not-upload_only trigger at once.
    """
    return _get_or_create_service(
        RESCAN_TRIGGER_SERVICE_NAME,
        (
            "Rescans a source's OwnCloud folder for new FITS files, runs StdWeb "
            "forced photometry on each, and uploads the result to SkyPortal -- "
            "the same thing any comment on the source already triggers. Click "
            '"Run Analysis" to trigger it directly, without posting a comment.'
        ),
        desired={
            "display_name": "Rescan OwnCloud",
            "upload_only": False,
            "url": f"{WEBHOOK_URL}/trigger",
            "display_on_resource_dropdown": True,
        },
    )


def restore_ledger(source_id: str) -> Tuple[List[str], List[str], Dict[str, dict]]:
    """
    Recover (processed_files, failed_files, file_details) for a source from its
    most recent *completed* entry against RESCAN_SERVICE_ID -- a save_ledger
    upload, written the same way regardless of which path (comment or
    webhook) triggered the run that produced it; see pipeline_tick. Empty/
    default if the source has never had one.

    Only "completed" entries count: an in-flight or failed upload shouldn't
    normally exist here (save_ledger always uploads with status "completed"
    immediately), but a stale leftover must not look like "this source has
    no ledger yet" to a rescan that happens to restore concurrently -- see
    prune_ledger_entries.

    Replaces the old mechanism of decoding hidden unicode variation selectors
    appended to every bot reply comment and unioning the result across all of
    them: a save always writes the full cumulative state in one entry, and
    prune_ledger_entries removes every older entry for the source, so trusting
    the single latest completed entry here is correct (not a union).
    """
    response = requests.get(
        f"{INSTANCE_URL}/api/obj/{source_id}/analysis",
        headers=header,
        params={"analysisServiceID": RESCAN_SERVICE_ID},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    completed = [e for e in response.json()["data"] if e.get("status") == "completed"]
    if not completed:
        return [], [], {}
    latest_id = max(e["id"] for e in completed)

    response = requests.get(
        f"{INSTANCE_URL}/api/obj/analysis/{latest_id}",
        headers=header,
        params={"includeAnalysisData": True},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()["data"].get("data") or {}
    results = data.get("results") or {}
    ledger = results.get("data") or {}
    return (
        ledger.get("processed_files", []),
        ledger.get("failed_files", []),
        ledger.get("file_details", {}),
    )


def prune_ledger_entries(source_id: str, service_id: int) -> None:
    """
    Delete every ObjAnalysis entry for this source against `service_id`
    except the newest (by id) -- keeps at most one at all times, well under
    SkyPortal's analysis_services.max_analysis_per_obj_per_user cap. Only
    ever called against RESCAN_SERVICE_ID (see save_ledger): correct there
    because save_ledger's same-source guard means at most one run -- and so
    at most one *live* entry -- ever exists for a source at a time. NOT
    safe for RESCAN_TRIGGER_SERVICE_ID, where several entries can be
    genuinely live at once (one running, one freshly queued in
    pending_rerun, one just superseded) -- see prune_stale_trigger_entries
    for that service instead.
    """
    response = requests.get(
        f"{INSTANCE_URL}/api/obj/{source_id}/analysis",
        headers=header,
        params={"analysisServiceID": service_id},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    entries = sorted(response.json()["data"], key=lambda e: e["id"])
    for stale in entries[:-1]:
        requests.delete(f"{INSTANCE_URL}/api/obj/analysis/{stale['id']}", headers=header, timeout=REQUEST_TIMEOUT).raise_for_status()


def prune_stale_trigger_entries(source_id: str) -> None:
    """
    Delete terminal (not "queued"/"pending") ObjAnalysis entries for this
    source against RESCAN_TRIGGER_SERVICE_ID, keeping only the single
    newest terminal one -- cleans up old "Run Analysis" results from the
    UI without ever touching an entry that's still in flight.

    Unlike RESCAN_SERVICE_ID (prune_ledger_entries, safe to reduce to "keep
    only the newest" because save_ledger's same-source guard means at most
    one run is ever live at a time), two or three RESCAN_TRIGGER_SERVICE_ID
    entries can be genuinely live at once now: one running, one freshly
    deferred into pending_rerun, one just superseded -- see
    _schedule_pending_rerun. Blindly keeping only the newest id would
    delete an in-flight sibling's row out from under it, so its own
    eventual callback would 403 with "Invalid token" (confirmed: this is
    exactly what the old shared prune_ledger_entries call used to do here).
    """
    response = requests.get(
        f"{INSTANCE_URL}/api/obj/{source_id}/analysis",
        headers=header,
        params={"analysisServiceID": RESCAN_TRIGGER_SERVICE_ID},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    terminal = sorted(
        (e for e in response.json()["data"] if e.get("status") not in ("queued", "pending")),
        key=lambda e: e["id"],
    )
    for stale in terminal[:-1]:
        requests.delete(f"{INSTANCE_URL}/api/obj/analysis/{stale['id']}", headers=header, timeout=REQUEST_TIMEOUT).raise_for_status()


def save_ledger(job: RescanJob) -> None:
    """
    Upload the current cumulative processed/failed file ledger for job.source_id
    as a new ObjAnalysis entry against RESCAN_SERVICE_ID (status "completed"
    immediately), then prune every older entry for this source -- see
    prune_ledger_entries. Called for every job regardless of trigger path
    (see pipeline_tick) -- this is the single source of truth restore_ledger
    reads back from; send_webhook_callback's own ObjAnalysis entry (against
    the separate RESCAN_TRIGGER_SERVICE_ID) is just SkyPortal's "Run
    Analysis" UI feedback, not read back from here.
    """
    payload = {
        "message": (
            f"{len(job.processed_files)} processed, {len(job.failed_files)} failed (as of job #{job.job_id}). "
            + "; ".join(describe_file_result(p, d.get("stdweb_id", -1), markdown=False) for p, d in job.file_details.items())
        ),
        # True (despite the name): gates whether SkyPortal's frontend shows
        # the "Analysis Results" accordion at all (see
        # SourceAnalysisPage.tsx) -- without it, results.data below is
        # uploaded but never rendered in the UI.
        "show_parameters": True,
        "show_plots": False,
        "show_corner": False,
        "analysis": {
            "results": {
                "format": "json",
                "data": {
                    "processed_files": job.processed_files,
                    "failed_files": job.failed_files,
                    "file_details": job.file_details,
                },
            }
        },
    }
    if SKYPORTAL_GROUP_IDS_FILTER:
        payload["group_ids"] = SKYPORTAL_GROUP_IDS_FILTER
    response = requests.post(
        f"{INSTANCE_URL}/api/obj/{job.source_id}/analysis_upload/{RESCAN_SERVICE_ID}",
        headers=header,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    prune_ledger_entries(job.source_id, RESCAN_SERVICE_ID)


def send_webhook_callback(job: RescanJob) -> None:
    """
    Report completion back to SkyPortal for a job triggered via its "Run
    Analysis" webhook (job.callback_url set -- see WebhookHandler) rather than
    a comment. Matches the contract at https://skyportal.io/docs/analysis.html:
    POST {"status", "message", "analysis": {"results": {...}}} to the
    callback_url embedded in the original trigger request; SkyPortal records
    this as a "completed" (success) or "failure" ObjAnalysis entry against
    RESCAN_TRIGGER_SERVICE_ID -- purely UI feedback for that button, not what
    restore_ledger reads (save_ledger, called unconditionally in
    pipeline_tick regardless of trigger path, is the actual ledger write).
    The callback token is single-use, so this always fires exactly once per
    run.
    """
    status = "success" if job.stage == 1000 else "failure"
    payload = {
        "status": status,
        "message": (
            f"{len(job.processed_files)} processed, {len(job.failed_files)} failed (as of job #{job.job_id}). "
            + "; ".join(describe_file_result(p, d.get("stdweb_id", -1), markdown=False) for p, d in job.file_details.items())
        ),
        "analysis": {
            "results": {
                "format": "json",
                "data": {
                    "processed_files": job.processed_files,
                    "failed_files": job.failed_files,
                    "file_details": job.file_details,
                },
            }
        },
    }
    try:
        # allow_redirects=False: job.callback_url already passed
        # _is_valid_callback_url's origin+route check when the job was
        # created, but a redirect response could still point the actual
        # POST (the full ledger payload) at an arbitrary third party.
        response = requests.post(job.callback_url, json=payload, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        response.raise_for_status()
    except Exception:
        traceback.print_exc()
        logger.error(f"Could not deliver webhook callback for job #{job.job_id} ({job.source_id})")
        return
    prune_stale_trigger_entries(job.source_id)


def notify_superseded(source_id: str, job_id: int, callback_url: str) -> None:
    """
    Tell SkyPortal that a webhook-triggered "Run Analysis" job was replaced
    by a newer trigger for the same source before it ever got to run -- see
    _schedule_pending_rerun's overwrite points in get_new_comments and
    main_loop. Without this, the superseded entry's ObjAnalysis record sits
    in "queued" forever: SkyPortal only transitions it when this token's
    callback_url is POSTed to, so the UI would otherwise show it stuck
    "running" until the analysis service's own 7200s timeout eventually
    marks it failed with no explanation (see _get_or_create_service's
    create_payload). There's no "superseded"/"cancelled" status in
    SkyPortal's webhook contract, only "success" or "failure" (see
    skyportal/handlers/api/webhook.py) -- "failure" is the honest one here,
    distinct from a genuine completed run.
    """
    payload = {
        "status": "failure",
        "message": (
            f"Superseded by a newer rescan trigger for this source before "
            f"job #{job_id} could run -- see the latest run's results instead."
        ),
    }
    try:
        response = requests.post(callback_url, json=payload, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        response.raise_for_status()
    except Exception:
        traceback.print_exc()
        logger.error(f"Could not deliver 'superseded' webhook callback for job #{job_id} ({source_id})")
        return
    prune_stale_trigger_entries(source_id)


def _schedule_pending_rerun(source_id: str, job_id: int, ra: float, dec: float, callback_url: str) -> None:
    """
    Set pending_rerun[source_id], first notifying (as superseded, see
    notify_superseded) whatever webhook trigger it's about to silently
    replace -- pending_rerun holds at most one entry per source, so an
    overwrite would otherwise just drop the old callback_url on the floor.
    Shared by get_new_comments (a comment coalescing over an existing
    pending entry) and main_loop's trigger_queue drain (a second "Run
    Analysis" trigger for a source already mid-rescan).
    """
    existing = pending_rerun.get(source_id)
    if existing is not None and existing[3]:
        notify_superseded(source_id, existing[0], existing[3])
    pending_rerun[source_id] = (job_id, ra, dec, callback_url)


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
    comments in one tick, only the latest *non-self-bot-reply* comment is used
    (this service's own replies always outrank the human comment that spawned
    them by id, so they'd otherwise win and silently swallow the real
    trigger); if a job for that source is already running (in `pipeline`), no
    job is created at all -- the latest comment id is queued in
    `pending_rerun` and fired the instant the active job finishes (see
    pipeline_tick_all), not on the next poll.

    Self-bot-reply exclusion and explicit-command detection here are purely
    structural (scheduling/routing only); actual command execution and
    files-restore are entirely pipeline_tick's stage 1 ("get the comments").

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
        timeout=REQUEST_TIMEOUT,
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
            if comment.get("author_id") == SELF_USER_ID and comment.get("bot") is not False:
                # Skip only this service's own *bot* replies (missing/null bot flag
                # treated conservatively as bot too) -- the admin user behind
                # SELF_USER_ID can still trigger a rescan with a real (non-bot,
                # e.g. UI-posted) comment of their own.
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
            _schedule_pending_rerun(source_id, comment["id"], source["ra"], source["dec"], "")
            continue
        new_jobs.append(RescanJob(
            source_id=source_id,
            job_id=comment["id"],
            stage=1,
            ra=source["ra"],
            dec=source["dec"],
        ))
    return new_jobs, last_iteration


def owncloud_folder_url(relative_path: str) -> str:
    """
    Browsable OwnCloud Files-app link for a SAVE_PATH-relative folder (e.g.
    "{source_id}/{telescope}") -- used by describe_file_result to enrich
    progress/completion messages.
    """
    return f"{BASE_URL}/apps/files/?dir=/{SAVE_PATH}/{relative_path}"


def describe_file_result(path: str, stdweb_id: int, markdown: bool) -> str:
    """
    One-line summary of a single file's outcome, as a link to its StdWeb
    task page (most useful: shows the actual photometry/logs) if one was
    ever created, else to its OwnCloud folder (e.g. it failed before
    reaching StdWeb at all). markdown=True for comment_back's reply
    (SkyPortal comments render markdown -- see package.json's
    react-markdown); markdown=False for save_ledger/send_webhook_callback's
    plain-text status message. Takes plain (path, stdweb_id) rather than a
    FileJob so it works equally for this run's FileJob objects and for
    job.file_details' cumulative, persisted entries (which cover files
    processed/failed in *previous* runs too -- see save_ledger).
    """
    filename = path.split("/")[-1]
    if stdweb_id and stdweb_id != -1:
        url = f"{STDWEB_URL}/tasks/{stdweb_id}"
    else:
        folder = "/".join(path.split("/")[:-1])
        url = owncloud_folder_url(folder)
    return f"[{filename}]({url})" if markdown else f"{filename} ({url})"


def comment_back(job: RescanJob, message: str) -> None:
    """
    Post a comment to SkyPortal to report the progress of the job. job.author
    is empty for a webhook-triggered job (no triggering comment to take it
    from -- see WebhookHandler), which drops the trailing @mention entirely.
    """
    text = f"{message} @{job.author}" if job.author else message
    response = requests.post(
        f"{INSTANCE_URL}/api/sources/{job.source_id}/comments",
        json={
            "text": text,
            #"group_ids": SKYPORTAL_GROUP_IDS_FILTER,
        },
        headers=header,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()


def owncloud_exists(path: str) -> bool:
    """
    Check if a file or folder exists and is accessible in OwnCloud.
    """
    check_url = f"{BASE_URL}/remote.php/dav/files/{OWNCLOUD_USER_ID}/{SAVE_PATH}/{path}"
    response = requests.request(
        "PROPFIND", check_url, auth=HTTPBasicAuth(OWNCLOUD_USERNAME, OWNCLOUD_TOKEN), headers={"Depth":"0"},
        timeout=REQUEST_TIMEOUT,
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
        "PROPFIND", check_url, auth=HTTPBasicAuth(OWNCLOUD_USERNAME, OWNCLOUD_TOKEN), headers={"Depth":str(depth)},
        timeout=REQUEST_TIMEOUT,
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

    Shared with STDWEB_GROUP_ID (a StdWeb Django Group *name*, not a
    numeric id -- StdWeb's own task-sharing field is name-keyed
    (api/serializers.py's TaskSerializer.groups is a SlugRelatedField on
    'name'), and StdWeb exposes no API to resolve a numeric group id to
    one) if configured, so a real operator with that StdWeb group can
    actually view the task via the link posted in rescan replies (see
    describe_file_result) -- without this, every task is owned solely by
    STDWEB_TOKEN's bot account and Task.can_view() denies everyone else
    (confirmed: StdWeb staff/view_all_tasks holders are the only
    exception). Unset by default: tasks stay bot-only, exactly as before.

    Returns task id of the new task.
    """
    data = {"original_name": f"{source_id}/{file_path.split('/')[-1]} : {job_id}"}
    if local_file:
        data["local_file"] = local_file
    if STDWEB_GROUP_ID:
        data["groups"] = [STDWEB_GROUP_ID]
    response = requests.request(
        "POST", f"{STDWEB_URL}/api/tasks/",
        headers={"Authorization": f"Token {STDWEB_TOKEN}"},
        data=data,
        timeout=REQUEST_TIMEOUT,
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
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()


def stdweb_task_status(stdweb_id: int) -> str:
    """
    Queries StdWeb task status.
    Returns task status as string.
    """
    response = requests.request(
        "GET", f"{STDWEB_URL}/api/tasks/{stdweb_id}/",
        headers={"Authorization": f"Token {STDWEB_TOKEN}"},
        timeout=REQUEST_TIMEOUT,
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
        timeout=REQUEST_TIMEOUT,
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
        "GET", download_url, auth=HTTPBasicAuth(OWNCLOUD_USERNAME, OWNCLOUD_TOKEN),
        timeout=FILE_TRANSFER_TIMEOUT,
    )
    response.raise_for_status()
    if stdweb_filename is None:
        stdweb_filename = owncloud_path.split("/")[-1]
    response2 = requests.request(
        "POST", f"{STDWEB_URL}/api/tasks/{stdweb_task_id}/files/{stdweb_filename}",
        headers={"Authorization": f"Token {STDWEB_TOKEN}"},
        files={"file": (stdweb_filename, response.content, "application/fits")},
        timeout=FILE_TRANSFER_TIMEOUT,
    )
    response2.raise_for_status()


# File-level retries (see pipeline_tick's stage 3) for a network-level
# hiccup -- StdWeb/SkyPortal momentarily unreachable, a timeout, a 5xx --
# rather than a genuine failure (StdWeb reports "failed", a 4xx, an
# unmapped telescope). failed_files is persisted via save_ledger and never
# reconsidered by restore_ledger's stage-2 diff, so treating a transient
# error the same as a permanent one would silently blacklist the file
# forever; a persistently broken file still fails for good after this many
# attempts.
MAX_FILE_RETRIES = 5


def _is_transient(e: Exception) -> bool:
    if isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
        return True
    if isinstance(e, requests.exceptions.HTTPError) and e.response is not None and e.response.status_code >= 500:
        return True
    return False


def pipeline_tick(job: RescanJob) -> bool:
    """
    Main function that runs for all jobs. Broken up into pieces to allow waiting for something to happen asynchronously (and continuing from checkpoints even after restarting python).

    Every comment on a source triggers a job stub (get_new_comments does zero
    content inspection). The whole flow below -- fetching/validating the actual
    trigger comment, restoring the files, listing OwnCloud, creating tasks -- is
    silent unless it finds something worth reporting: new files, a failure, or
    the triggering comment itself mentions "owncloud"/"upload" (in which case a
    "nothing new" reply is sent too, so a human who says they uploaded something
    gets confirmation either way). Explicit "{SKYPORTAL_COMMAND}help/dump"
    commands always reply.

    Flow (labelled by job.stage):
    1. Get the comments (one API call): find the trigger comment, bail out
       silently if it's this service's own bot reply (see get_new_comments and
       the author_id == SELF_USER_ID and bot checks there and below) or was
       deleted,
       parse an explicit command if the comment mentions one, and restore the
       processed/failed file ledger (see restore_ledger -- a dedicated Analysis
       Service entry per source, not decoded from past comments)
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
        if job.stage <= 1:  # Immediately after seeing the comment (or webhook trigger)
            if job.callback_url:
                # Triggered via the "Run Analysis" webhook (see WebhookHandler),
                # not a comment -- no comment to fetch or command to parse.
                pass
            else:
                response = requests.get(
                    f"{INSTANCE_URL}/api/sources/{job.source_id}/comments",
                    headers=header,
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                comments = response.json()["data"]

                trigger = next((c for c in comments if c["id"] == job.job_id), None)
                if trigger is None or (
                    trigger.get("author_id") == SELF_USER_ID
                    and trigger.get("bot") is not False
                ):
                    # Comment vanished, or is this service's own *bot* reply (missing/
                    # null bot flag treated conservatively as bot too) -- a second,
                    # independent check (get_new_comments already filters its own
                    # bot replies out, but the trigger comment is re-fetched fresh
                    # here since pipeline execution can lag behind polling), guarding
                    # against a self-triggering reply loop while still letting the
                    # admin user behind SELF_USER_ID trigger one with a real comment.
                    job.stage = 1000
                    return True
                job.comment = trigger["text"]
                job.author = trigger["author_id"]

                if SKYPORTAL_COMMAND in job.comment:
                    command = job.comment.split(SKYPORTAL_COMMAND)[1]
                    argv = command.split(" ")
                    help_message = f"Usage: any comment triggers a rescan of this source's OwnCloud directory. Feedback is silent unless new files are found (or fail), or your comment mentions 'owncloud'/'upload'.\nExplicit commands (no space before the keyword):\n{SKYPORTAL_COMMAND}help\n{SKYPORTAL_COMMAND}dump [limit]."
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

            job.processed_files, job.failed_files, job.file_details = restore_ledger(job.source_id)
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
                            job.file_details[file_job.path] = {"stdweb_id": file_job.stdweb_id, "status": "failed", "error": file_job.error}
                            job.unsaved_results.append(file_job)
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
                            if _is_transient(e):
                                # Re-raise unwrapped so the outer except below
                                # can still recognize and retry it.
                                raise
                            raise RuntimeError(f"SkyPortal upload failed: {e} ({task_link})")
                        file_job.stage = 1000
                        job.processed_files.append(file_job.path)
                        job.file_details[file_job.path] = {"stdweb_id": file_job.stdweb_id, "status": "processed"}
                        job.unsaved_results.append(file_job)
                except Exception as e:
                    if _is_transient(e) and file_job.retry_count < MAX_FILE_RETRIES:
                        # file_job.stage is left untouched, so the next tick
                        # resumes the same call rather than permanently
                        # blacklisting the file -- see MAX_FILE_RETRIES.
                        file_job.retry_count += 1
                        logger.warning(
                            f"Transient error on {file_job.path} (attempt {file_job.retry_count}/{MAX_FILE_RETRIES}), "
                            f"retrying next tick: {e}"
                        )
                        continue
                    file_job.stage = 1001
                    file_job.error = str(e)
                    job.failed_files.append(file_job.path)
                    job.file_details[file_job.path] = {"stdweb_id": file_job.stdweb_id, "status": "failed", "error": file_job.error}
                    job.unsaved_results.append(file_job)

            if job.unsaved_results:
                newly_done = [f for f in job.unsaved_results if f.stage == 1000]
                newly_failed = [f for f in job.unsaved_results if f.stage == 1001]
                try:
                    # Save before notifying, and independently of whether the
                    # notification succeeds: a processed/failed file must be
                    # durably recorded in the ledger even if the progress
                    # comment fails to post, or a future restore_ledger would
                    # never learn about it and silently reprocess it
                    # (duplicate StdWeb task, duplicate SkyPortal photometry
                    # upload) on the next rescan. Retried every tick
                    # (job.unsaved_results stays populated) until it succeeds.
                    save_ledger(job)
                except Exception:
                    traceback.print_exc()
                    logger.error(f"Could not save ledger for job #{job.job_id} ({job.source_id}); will retry next tick")
                else:
                    job.unsaved_results = []
                    parts = []
                    if newly_done:
                        parts.append(f"{len(newly_done)} done: " + ", ".join(describe_file_result(f.path, f.stdweb_id, markdown=True) for f in newly_done))
                    if newly_failed:
                        parts.append(f"{len(newly_failed)} failed: " + ", ".join(describe_file_result(f.path, f.stdweb_id, markdown=True) for f in newly_failed))
                    try:
                        comment_back(job, "; ".join(parts) + ".")
                    except Exception:
                        # Best-effort only -- the ledger save above already
                        # succeeded, so no state is lost if this particular
                        # notification comment doesn't make it.
                        traceback.print_exc()
                        logger.error(f"Could not post progress comment for job #{job.job_id} ({job.source_id})")

            if all(file_job.stage >= 1000 for file_job in job.file_jobs) and not job.unsaved_results:
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
        summary = (
            f"Rescan complete: {new_done} new file(s) processed and uploaded to SkyPortal, {new_failed} new file(s) failed. "
            f"Total: {len(job.processed_files)} processed, {len(job.failed_files)} failed."
        )
        details = "; ".join(describe_file_result(p, d.get("stdweb_id", -1), markdown=True) for p, d in job.file_details.items())
        comment_back(job, f"{summary}\n{details}" if details else summary)
    return True

def pipeline_tick_all() -> None:
    """
    Run all tasks in the pipeline and remove the ones that have finished. When a
    job finishes, if another comment or webhook trigger arrived for its source
    while it was running (queued in pending_rerun by get_new_comments or
    main_loop's trigger_queue drain), start that rerun immediately rather than
    waiting for the next poll. A finished webhook-triggered job (job.callback_url
    set) also reports its result back to SkyPortal -- see send_webhook_callback.
    """
    i = 0
    while i < len(pipeline):
        job = pipeline[i]
        if pipeline_tick(job):
            if job.callback_url:
                send_webhook_callback(job)
            pipeline.pop(i)
            queued = pending_rerun.pop(job.source_id, None)
            if queued is not None:
                job_id, ra, dec, callback_url = queued
                pipeline.append(RescanJob(source_id=job.source_id, job_id=job_id, stage=1, ra=ra, dec=dec, callback_url=callback_url))
        else:
            i += 1

# SkyPortal's own callback_url format (see
# https://skyportal.io/docs/analysis.html and skyportal/utils/app.py's
# get_app_base_url/handled_by_url): {app_base_url}/api/webhook/obj_analysis/{token}.
_CALLBACK_PATH_RE = re.compile(r"^/api/webhook/obj_analysis/[^/]+$")


def _is_valid_callback_url(url: str) -> bool:
    """
    WebhookHandler.do_POST's request body supplies both resource_id and
    callback_url verbatim -- without this check, anyone who can reach
    WEBHOOK_LISTEN_HOST:WEBHOOK_LISTEN_PORT (0.0.0.0 by default) could name
    any real source_id with their own callback_url; pipeline_tick would then
    read that source's full ledger (file paths, StdWeb task ids, error
    text) using this service's own SkyPortal credentials and
    send_webhook_callback would hand all of it to the attacker's server.
    Restrict callback_url to exactly INSTANCE_URL's own origin and
    SkyPortal's documented callback route.
    """
    expected = urlparse(INSTANCE_URL)
    parsed = urlparse(url)
    return (
        parsed.scheme == expected.scheme
        and parsed.netloc == expected.netloc
        and bool(_CALLBACK_PATH_RE.match(parsed.path))
    )


class WebhookHandler(BaseHTTPRequestHandler):
    """
    Receives SkyPortal's "Run Analysis" webhook call for RESCAN_TRIGGER_SERVICE_ID
    (see get_or_create_trigger_service/run_webhook_server) and turns it into a
    RescanJob on trigger_queue, same as a comment does for get_new_comments.
    Runs on its own thread -- see run_webhook_server -- so it never touches
    `pipeline` directly (that's main_loop's job, draining trigger_queue and
    applying the same already-running-source guard as comments; see
    main_loop).

    do_POST rejects (400) a request whose callback_url doesn't pass
    _is_valid_callback_url -- without that check this endpoint would let
    anyone who can reach it read any source's ledger via a callback_url of
    their choosing. Otherwise it expects an immediate 200 acknowledging
    receipt (SkyPortal records anything else as a failed run); the actual
    rescan happens later, asynchronously, same as any other job in the
    pipeline. See send_webhook_callback for how/when the result is reported
    back.
    """

    def log_message(self, format, *args):
        pass  # comment_watcher.py already logs; skip BaseHTTPRequestHandler's default access log

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            source_id = body.get("resource_id")
            callback_url = body.get("callback_url")
            obj = (body.get("inputs") or {}).get("obj") or {}
            if not source_id or not callback_url or not _is_valid_callback_url(callback_url):
                self.send_response(400)
                self.end_headers()
                return
            trigger_queue.put(RescanJob(
                source_id=source_id,
                job_id=int(time.time() * 1000),
                stage=1,
                ra=obj.get("ra", 0),
                dec=obj.get("dec", 0),
                callback_url=callback_url,
            ))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "success"}')
        except Exception:
            traceback.print_exc()
            self.send_response(500)
            self.end_headers()


def run_webhook_server(host: str, port: int) -> None:
    """
    Starts the "Run Analysis" webhook listener on a daemon thread. Only
    called when WEBHOOK_URL is configured -- see get_or_create_trigger_service
    and the startup block below.
    """
    server = ThreadingHTTPServer((host, port), WebhookHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"Webhook listener on http://{host}:{port}")


def main_loop(start_time: datetime) -> None:
    """
    Main monitoring loop that watches for new comments, creates and runs jobs.

    Continuously polls SkyPortal and runs active pipeline jobs. Also drains
    trigger_queue (webhook-triggered jobs queued by WebhookHandler, on its own
    thread) into `pipeline` -- this is the only thread that mutates `pipeline`.

    Args:
        start_time: Datetime to start monitoring from
    """

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

            while not trigger_queue.empty():
                job = trigger_queue.get_nowait()
                active_sources = {j.source_id for j in pipeline}
                if job.source_id in active_sources:
                    # Same coalescing guard as comment-triggered jobs (see
                    # get_new_comments) -- without it, a second "Run Analysis"
                    # click (or a retried webhook delivery) for a source
                    # already mid-rescan starts a fully independent second
                    # job: duplicate StdWeb tasks, duplicate SkyPortal
                    # photometry uploads, and a last-writer-wins race on
                    # save_ledger. _schedule_pending_rerun also notifies
                    # (as superseded) whatever earlier pending trigger this
                    # one replaces, so it doesn't sit "queued" forever in
                    # SkyPortal's UI.
                    _schedule_pending_rerun(job.source_id, job.job_id, job.ra, job.dec, job.callback_url)
                    logger.info(f"Webhook trigger for {job.source_id} deferred -- already running")
                    continue
                logger.info(f"New webhook trigger for {job.source_id}")
                pipeline.append(job)

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

    # LOG_LEVEL is read only now, not at import time, so it can come from
    # --env-file as well as an already-exported shell variable; only this
    # service's own logger is affected (see the module-level
    # logging.basicConfig/logger setup above) -- third-party libraries
    # (requests, urllib3) stay at the default WARNING level regardless.
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, None)
    if not isinstance(log_level, int):
        logger.warning(f"Invalid LOG_LEVEL {log_level_name!r}, falling back to INFO")
        log_level = logging.INFO
    logger.setLevel(log_level)

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
        # StdWeb Django Group *name* (see stdweb_make_task) tasks get shared
        # with -- unset by default, meaning every created task stays visible
        # only to STDWEB_TOKEN's own bot account (and StdWeb staff/
        # view_all_tasks holders), exactly as before this setting existed.
        STDWEB_GROUP_ID = os.getenv("STDWEB_GROUP_ID", "")
        SOURCE_TAG = os.getenv("SOURCE_TAG", "")
        POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
        SKYPORTAL_GROUP_IDS_FILTER = [
            int(x.strip()) for x in os.getenv("GROUP_IDS", "").split(",") if x.strip()
        ]
        SKYPORTAL_COMMAND=os.getenv("SKYPORTAL_COMMAND", "@rescan")
        # Externally-reachable base URL for this process's own HTTP listener
        # (e.g. "http://100.90.108.127:8765"), so SkyPortal's "Run Analysis"
        # button can trigger a rescan directly, not just via a comment -- see
        # WebhookHandler/get_or_create_trigger_service. Unset (default): the
        # separate RESCAN_TRIGGER_SERVICE_NAME service is never created.
        WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
        WEBHOOK_LISTEN_HOST = os.getenv("WEBHOOK_LISTEN_HOST", "0.0.0.0")
        WEBHOOK_LISTEN_PORT = int(os.getenv("WEBHOOK_LISTEN_PORT", "8765"))
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
    # Used to recognize (and skip) this service's own *bot* reply comments,
    # so it never reacts to itself -- see get_new_comments/pipeline_tick's
    # "author_id == SELF_USER_ID and bot" checks. The admin user behind this
    # token can still trigger a rescan with a real (non-bot) comment of their
    # own; only automated replies posted under this same identity are
    # excluded. Resolved once at startup rather than hardcoded so
    # SKYPORTAL_TOKEN alone determines identity.
    response = requests.get(f"{INSTANCE_URL}/api/internal/profile", headers=header, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    SELF_USER_ID = response.json()["data"]["id"]

    try:
        RESCAN_SERVICE_ID = get_or_create_rescan_service()
        if WEBHOOK_URL:
            RESCAN_TRIGGER_SERVICE_ID = get_or_create_trigger_service()
    except Exception as e:
        logger.error(f"Could not find or create the {RESCAN_SERVICE_NAME!r}/{RESCAN_TRIGGER_SERVICE_NAME!r} analysis service: {e}")
        exit(1)
    if WEBHOOK_URL:
        run_webhook_server(WEBHOOK_LISTEN_HOST, WEBHOOK_LISTEN_PORT)

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
