import os
import shutil
import subprocess
import sys
import time
import uuid

import pytest
import requests
from dotenv import load_dotenv

load_dotenv("../.env")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from comment_watcher import RESCAN_SERVICE_NAME, RESCAN_TRIGGER_SERVICE_NAME  # noqa: E402

# OwnCloud's on-disk data directory, as bind-mounted by docker-compose.yaml.
# Used only by seed_owncloud_file()'s local-run fallback below -- CI places
# the fixture itself via `docker cp` (see "Seed OwnCloud fixture file" in
# skyportal_processor_service_ci.yaml), never through OwnCloud's write API.
OWNCLOUD_DATA_PATH = os.getenv("OWNCLOUD_DATA_PATH", "../data/owncloud")
OWNCLOUD_USER_ID = os.getenv("OWNCLOUD_USER_ID")
SAVE_PATH = os.getenv("SAVE_PATH")

SKYPORTAL_URL = os.getenv("SKYPORTAL_URL")
SKYPORTAL_TOKEN = os.getenv("SKYPORTAL_TOKEN")
SKYPORTAL_HEADERS = {"Authorization": f"token {SKYPORTAL_TOKEN}"}
# Belongs to a second user, distinct from comment_watcher.py's own
# SKYPORTAL_TOKEN -- see post_comment's docstring for why that's required.
SKYPORTAL_POSTER_TOKEN = os.getenv("SKYPORTAL_POSTER_TOKEN")
SKYPORTAL_POSTER_HEADERS = {"Authorization": f"token {SKYPORTAL_POSTER_TOKEN}"}

STDWEB_URL = os.getenv("STDWEB_URL")
STDWEB_TOKEN = os.getenv("STDWEB_TOKEN")
STDWEB_HEADERS = {"Authorization": f"Token {STDWEB_TOKEN}"}

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "fixtures", "test_image.fits")
# Real WCS header of the fixture file -- must match, or forced photometry looks
# for the target at the wrong position. Source: karpov-sv/gmadet (MIT), see
# fixtures/README.md for full attribution.
FIXTURE_RA = 244.000112892
FIXTURE_DEC = 22.2673593981

# CI's "Seed OwnCloud fixture file" step generates this and seeds the fixture
# file for every id in TEST_SOURCE_SUFFIXES below (via `docker cp`, not this
# test) before pytest runs -- it has to know the ids up front to pick
# destination paths. Falls back to a locally-generated base for
# standalone/local runs, where seed_owncloud_file() below does the
# equivalent placement itself.
BASE_SOURCE_ID = os.getenv("TEST_SOURCE_ID") or f"CIRESCANTEST{uuid.uuid4().hex[:8]}"
TELESCOPE_FOLDER = "TAROT-TCA"

# One source per trigger path under test, all sharing the one fixture image
# (same real field/catalogue data, just two independent source rows so the
# two tests below don't share mutable state and can run concurrently -- see
# skyportal_processor_service_ci.yaml's `pytest -n` invocation). Add another
# suffix here (and a matching test) for further trigger paths.
TEST_SOURCE_SUFFIXES = ["COMMENT", "ANALYSIS"]
COMMENT_SOURCE_ID = f"{BASE_SOURCE_ID}-COMMENT"
ANALYSIS_SOURCE_ID = f"{BASE_SOURCE_ID}-ANALYSIS"


def sp_request(method, path, **kwargs):
    """
    requests.request against SkyPortal with 429 retry/backoff. Running both
    tests below concurrently (see skyportal_processor_service_ci.yaml's
    `pytest -n 2`) roughly doubles ordinary polling traffic against the one
    SkyPortal instance, which is enough to trip its rate limiter on its own
    -- not just under genuine abuse -- so a plain requests.get/post here
    would otherwise make the whole run flaky. Callers still call
    response.raise_for_status() themselves for any other error.
    """
    url = f"{SKYPORTAL_URL}{path}"
    response = requests.request(method, url, **kwargs)
    attempt = 0
    while response.status_code == 429 and attempt < 5:
        time.sleep(2 * (attempt + 1))
        response = requests.request(method, url, **kwargs)
        attempt += 1
    return response


def seed_owncloud_file(relative_path, fixture_path):
    """
    Place a file directly in OwnCloud's on-disk storage, bypassing OwnCloud's
    own write API entirely (no PUT/MKCOL) -- see docker-compose.yaml's
    owncloud service, which bind-mounts its data dir to ./data/owncloud so
    this is possible. OwnCloud indexes files into a DB-backed cache separate
    from the raw filesystem, so a `files:scan` is still needed afterward for
    WebDAV/PROPFIND (what comment_watcher.py reads with) to see it -- that's
    a local `docker exec` maintenance command, not a network write against
    OwnCloud's API.
    """
    # Layout is {datadir}/{user}/files/{path} relative to the configured
    # `datadirectory` -- which itself defaults to {OWNCLOUD_DATA_PATH}/files
    # (confirmed via `occ config:system:get datadirectory` against this same
    # owncloud/server:10.15 image), not {OWNCLOUD_DATA_PATH} directly.
    dest = os.path.join(
        OWNCLOUD_DATA_PATH, "files", OWNCLOUD_USER_ID, "files", relative_path
    )
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copyfile(fixture_path, dest)

    subprocess.run(
        ["docker", "exec", "owncloud_server", "occ", "files:scan", OWNCLOUD_USER_ID],
        check=True,
    )


def post_comment(source_id, text):
    """
    Post a comment as a real, distinct SkyPortal user -- comment_watcher.py
    filters out comments authored by its own user id (see SELF_USER_ID in
    comment_watcher.py), not by SkyPortal's bot flag (every token-authenticated
    request is bot=True regardless of which token -- see
    skyportal/handlers/api/comment.py: is_bot_request = isinstance(current_user,
    Token) -- so that flag alone can't distinguish "a human" from "myself").
    SKYPORTAL_POSTER_TOKEN belongs to a second user (see "Create second
    SkyPortal user" in the CI workflow) specifically so this is a genuine
    plain API call, with no browser/UI involved at all.
    """
    response = sp_request(
        "POST", f"/api/sources/{source_id}/comments",
        json={"text": text},
        headers=SKYPORTAL_POSTER_HEADERS,
    )
    response.raise_for_status()


def get_comments(source_id):
    response = sp_request(
        "GET", f"/api/sources/{source_id}/comments",
        headers=SKYPORTAL_HEADERS,
    )
    response.raise_for_status()
    return response.json()["data"]


def self_user_id():
    """The user id behind SKYPORTAL_TOKEN -- comment_watcher.py's own
    SELF_USER_ID, resolved the same way (GET /api/internal/profile)."""
    response = sp_request("GET", "/api/internal/profile", headers=SKYPORTAL_HEADERS)
    response.raise_for_status()
    return response.json()["data"]["id"]


def get_service_id(name):
    response = sp_request("GET", "/api/analysis_service", headers=SKYPORTAL_HEADERS)
    response.raise_for_status()
    return next(s["id"] for s in response.json()["data"] if s["name"] == name)


def get_ledger(source_id):
    """
    Fetch the current processed/failed file ledger for a source directly from
    the Analysis Service comment_watcher.py persists it in (see
    save_ledger/restore_ledger in comment_watcher.py) -- replaces decoding it
    out of a bot reply comment. Mirrors restore_ledger's own "latest
    completed entry" logic.
    """
    service_id = get_service_id(RESCAN_SERVICE_NAME)
    response = sp_request(
        "GET", f"/api/obj/{source_id}/analysis",
        headers=SKYPORTAL_HEADERS,
        params={"analysisServiceID": service_id},
    )
    response.raise_for_status()
    completed = [e for e in response.json()["data"] if e.get("status") == "completed"]
    assert completed, f"No completed ledger entry for {source_id}"
    latest_id = max(e["id"] for e in completed)

    response = sp_request(
        "GET", f"/api/obj/analysis/{latest_id}",
        headers=SKYPORTAL_HEADERS,
        params={"includeAnalysisData": True},
    )
    response.raise_for_status()
    return response.json()["data"]["data"]["results"]["data"]


@pytest.fixture(scope="module", autouse=True)
def seed_sources_and_files():
    """Real SkyPortal sources at the fixture's coordinates (one per
    TEST_SOURCE_SUFFIXES entry), each with the fixture FITS already sitting
    in the OwnCloud folder comment_watcher.py scans for it."""
    for source_id in (COMMENT_SOURCE_ID, ANALYSIS_SOURCE_ID):
        response = sp_request(
            "POST", "/api/sources",
            json={"id": source_id, "ra": FIXTURE_RA, "dec": FIXTURE_DEC, "group_ids": [1]},
            headers=SKYPORTAL_HEADERS,
        )
        if response.status_code == 400:
            # This fixture runs once per pytest-xdist worker process (each
            # imports the test module independently), and every worker seeds
            # both sources regardless of which test it actually runs -- so
            # two workers can race to insert the same new source id. Treat
            # "already exists" as success rather than a real failure.
            check = sp_request("GET", f"/api/sources/{source_id}", headers=SKYPORTAL_HEADERS)
            if check.status_code == 200:
                continue
        response.raise_for_status()

        if not os.getenv("TEST_SOURCE_ID"):
            # Standalone/local run: CI's "Seed OwnCloud fixture file" step didn't
            # already place it (TEST_SOURCE_ID unset means BASE_SOURCE_ID above
            # was generated locally), so do it here instead.
            seed_owncloud_file(
                f"{SAVE_PATH}/{source_id}/{TELESCOPE_FOLDER}/test_image.fits", FIXTURE_PATH
            )

    yield
    # No cleanup: this only runs against ephemeral CI containers (owncloud,
    # skyportal, stdweb) that are torn down (`docker compose down -v`) right
    # after the test job finishes.


def wait_for_reply(source_id, after_comment_id, timeout=240, interval=5):
    """Poll until comment_watcher.py's bot reply lands on the thread."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for comment in get_comments(source_id):
            if (
                comment["id"] > after_comment_id
                and comment.get("bot") is True
                and "Rescan complete" in comment["text"]
            ):
                return comment
        time.sleep(interval)
    raise TimeoutError(f"No rescan reply for {source_id} within {timeout}s")


def wait_for_analysis_completion(source_id, service_id, after_id=0, timeout=240, interval=5):
    """
    Poll for a new "completed"/"failure" ObjAnalysis entry. Webhook-triggered
    jobs (job.callback_url set in comment_watcher.py) don't post a reply
    comment -- they report back via send_webhook_callback, which SkyPortal
    records as an entry here instead; this is that path's equivalent of
    wait_for_reply.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = sp_request(
            "GET", f"/api/obj/{source_id}/analysis",
            headers=SKYPORTAL_HEADERS,
            params={"analysisServiceID": service_id},
        )
        response.raise_for_status()
        for entry in response.json()["data"]:
            if entry["id"] > after_id and entry.get("status") in ("completed", "failure"):
                return entry
        time.sleep(interval)
    raise TimeoutError(f"No analysis completion for {source_id} within {timeout}s")


def wait_for_task_state(task_id, states, timeout=240, interval=5):
    deadline = time.time() + timeout
    last_state = None
    while time.time() < deadline:
        response = requests.get(
            f"{STDWEB_URL}/api/tasks/{task_id}/", headers=STDWEB_HEADERS
        )
        response.raise_for_status()
        last_state = response.json()["state"]
        if last_state in states:
            return last_state
        time.sleep(interval)
    raise TimeoutError(
        f"StdWeb task {task_id} still in state {last_state!r} after {timeout}s"
    )


def assert_file_processed(source_id):
    """
    Shared post-condition for both trigger paths below: exactly the fixture
    file processed, the StdWeb task it spawned finished successfully, and the
    resulting photometry point actually landed on the SkyPortal source (not
    merely claimed processed by a bookkeeping bug).
    """
    file_path = f"{source_id}/{TELESCOPE_FOLDER}/test_image.fits"
    ledger = get_ledger(source_id)
    assert ledger["processed_files"] == [file_path], ledger
    assert ledger["failed_files"] == [], ledger
    file_detail = ledger["file_details"].get(file_path)
    assert file_detail is not None and file_detail["status"] == "processed", ledger
    stdweb_id = file_detail["stdweb_id"]

    final_state = wait_for_task_state(stdweb_id, {"photometry_done", "failed"})
    assert final_state == "photometry_done"

    response = sp_request(
        "GET", f"/api/sources/{source_id}/photometry", headers=SKYPORTAL_HEADERS
    )
    response.raise_for_status()
    photometry = response.json()["data"]
    matching = [p for p in photometry if p.get("origin") == f"stdweb:{stdweb_id}"]
    assert matching, f"No SkyPortal photometry point with origin stdweb:{stdweb_id}"


def test_comment_triggers_rescan_and_photometry():
    """
    End-to-end contract: a plain (non-command) comment from a user other than
    comment_watcher.py's own makes it create+run a StdWeb forced-photometry
    task for the source's new OwnCloud FITS file, upload the result to
    SkyPortal as a photometry point (via StdWeb's POST
    /api/tasks/{id}/skyportal/), and reply on the thread with the outcome --
    see "any comment not from SELF_USER_ID triggers a rescan" and stage 7 in
    comment_watcher.py.
    """
    before_ids = {c["id"] for c in get_comments(COMMENT_SOURCE_ID)}
    post_comment(COMMENT_SOURCE_ID, "please check for new data")
    new_comments = [c for c in get_comments(COMMENT_SOURCE_ID) if c["id"] not in before_ids]
    assert new_comments, "comment did not appear via the API"
    trigger = new_comments[0]
    assert trigger["author_id"] != self_user_id(), (
        "trigger comment must be from a different user than comment_watcher.py's "
        "own SKYPORTAL_TOKEN to be picked up"
    )
    comment_id = trigger["id"]

    reply = wait_for_reply(COMMENT_SOURCE_ID, comment_id)
    assert "1 new file(s) processed and uploaded to SkyPortal" in reply["text"], reply["text"]
    assert "0 new file(s) failed" in reply["text"], reply["text"]

    assert_file_processed(COMMENT_SOURCE_ID)


def test_analysis_triggers_rescan_and_photometry():
    """
    End-to-end contract for the other trigger path: SkyPortal's own "Run
    Analysis" button/API (POST /api/obj/<source_id>/analysis/<id>) against
    the separate grandma_rescan_trigger Analysis Service runs the same
    pipeline as a comment, without posting one -- see
    WebhookHandler/send_webhook_callback in comment_watcher.py. The actual
    ledger (checked below via assert_file_processed) is still written to the
    main grandma_rescan service, same as a comment-triggered run -- see
    save_ledger. Needs WEBHOOK_URL configured (see
    skyportal_processor_service_ci.yaml's "Create test environment file");
    skipped if it isn't, since there's then no registered trigger service
    for SkyPortal to call.
    """
    if not os.getenv("WEBHOOK_URL"):
        pytest.skip("WEBHOOK_URL not configured -- analysis-triggered path is inactive")

    service_id = get_service_id(RESCAN_TRIGGER_SERVICE_NAME)
    response = sp_request(
        "GET", f"/api/obj/{ANALYSIS_SOURCE_ID}/analysis",
        headers=SKYPORTAL_HEADERS,
        params={"analysisServiceID": service_id},
    )
    response.raise_for_status()
    before_id = max((e["id"] for e in response.json()["data"]), default=0)

    response = sp_request(
        "POST", f"/api/obj/{ANALYSIS_SOURCE_ID}/analysis/{service_id}",
        json={},
        headers=SKYPORTAL_HEADERS,
    )
    response.raise_for_status()

    entry = wait_for_analysis_completion(ANALYSIS_SOURCE_ID, service_id, after_id=before_id)
    assert entry["status"] == "completed", entry

    assert_file_processed(ANALYSIS_SOURCE_ID)
