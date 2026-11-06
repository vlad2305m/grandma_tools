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
from comment_watcher import RESCAN_SERVICE_NAME  # noqa: E402

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

# CI's "Seed OwnCloud fixture file" step generates this and places the
# fixture file under it (via `docker cp`, not this test) before pytest runs
# -- it has to know the id up front to pick a destination path. Falls back
# to a locally-generated one for standalone/local runs, where
# seed_owncloud_file() below does the equivalent placement itself.
SOURCE_ID = os.getenv("TEST_SOURCE_ID") or f"CIRESCANTEST{uuid.uuid4().hex[:8]}"
TELESCOPE_FOLDER = "TAROT-TCA"


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
    response = requests.post(
        f"{SKYPORTAL_URL}/api/sources/{source_id}/comments",
        json={"text": text},
        headers=SKYPORTAL_POSTER_HEADERS,
    )
    response.raise_for_status()


def get_comments():
    response = requests.get(
        f"{SKYPORTAL_URL}/api/sources/{SOURCE_ID}/comments",
        headers=SKYPORTAL_HEADERS,
    )
    response.raise_for_status()
    return response.json()["data"]


def self_user_id():
    """The user id behind SKYPORTAL_TOKEN -- comment_watcher.py's own
    SELF_USER_ID, resolved the same way (GET /api/internal/profile)."""
    response = requests.get(f"{SKYPORTAL_URL}/api/internal/profile", headers=SKYPORTAL_HEADERS)
    response.raise_for_status()
    return response.json()["data"]["id"]


def get_ledger(source_id):
    """
    Fetch the current processed/failed file ledger for a source directly from
    the Analysis Service comment_watcher.py persists it in (see
    save_ledger/restore_ledger in comment_watcher.py) -- replaces decoding it
    out of a bot reply comment. Mirrors restore_ledger's own "latest
    completed entry" logic.
    """
    response = requests.get(f"{SKYPORTAL_URL}/api/analysis_service", headers=SKYPORTAL_HEADERS)
    response.raise_for_status()
    service_id = next(
        s["id"] for s in response.json()["data"] if s["name"] == RESCAN_SERVICE_NAME
    )

    response = requests.get(
        f"{SKYPORTAL_URL}/api/obj/{source_id}/analysis",
        headers=SKYPORTAL_HEADERS,
        params={"analysisServiceID": service_id},
    )
    response.raise_for_status()
    completed = [e for e in response.json()["data"] if e.get("status") == "completed"]
    assert completed, f"No completed ledger entry for {source_id}"
    latest_id = max(e["id"] for e in completed)

    response = requests.get(
        f"{SKYPORTAL_URL}/api/obj/analysis/{latest_id}",
        headers=SKYPORTAL_HEADERS,
        params={"includeAnalysisData": True},
    )
    response.raise_for_status()
    return response.json()["data"]["data"]["results"]["data"]


@pytest.fixture(scope="module", autouse=True)
def seed_source_and_file():
    """Real SkyPortal source at the fixture's coordinates, with the fixture FITS
    already sitting in the OwnCloud folder comment_watcher.py scans for it."""
    response = requests.post(
        f"{SKYPORTAL_URL}/api/sources",
        json={"id": SOURCE_ID, "ra": FIXTURE_RA, "dec": FIXTURE_DEC, "group_ids": [1]},
        headers=SKYPORTAL_HEADERS,
    )
    response.raise_for_status()

    if not os.getenv("TEST_SOURCE_ID"):
        # Standalone/local run: CI's "Seed OwnCloud fixture file" step didn't
        # already place it (TEST_SOURCE_ID unset means SOURCE_ID above was
        # generated locally), so do it here instead.
        seed_owncloud_file(
            f"{SAVE_PATH}/{SOURCE_ID}/{TELESCOPE_FOLDER}/test_image.fits", FIXTURE_PATH
        )

    yield
    # No cleanup: this only runs against ephemeral CI containers (owncloud,
    # skyportal, stdweb) that are torn down (`docker compose down -v`) right
    # after the test job finishes.


def wait_for_reply(after_comment_id, timeout=240, interval=5):
    """Poll until comment_watcher.py's bot reply lands on the thread."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for comment in get_comments():
            if (
                comment["id"] > after_comment_id
                and comment.get("bot") is True
                and "Rescan complete" in comment["text"]
            ):
                return comment
        time.sleep(interval)
    raise TimeoutError(f"No rescan reply for {SOURCE_ID} within {timeout}s")


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
    before_ids = {c["id"] for c in get_comments()}
    post_comment(SOURCE_ID, "please check for new data")
    new_comments = [c for c in get_comments() if c["id"] not in before_ids]
    assert new_comments, "comment did not appear via the API"
    trigger = new_comments[0]
    assert trigger["author_id"] != self_user_id(), (
        "trigger comment must be from a different user than comment_watcher.py's "
        "own SKYPORTAL_TOKEN to be picked up"
    )
    comment_id = trigger["id"]

    reply = wait_for_reply(comment_id)
    assert "1 new file(s) processed and uploaded to SkyPortal" in reply["text"], reply["text"]
    assert "0 new file(s) failed" in reply["text"], reply["text"]

    file_path = f"{SOURCE_ID}/{TELESCOPE_FOLDER}/test_image.fits"
    ledger = get_ledger(SOURCE_ID)
    assert ledger["processed_files"] == [file_path], ledger
    assert ledger["failed_files"] == [], ledger
    file_detail = ledger["file_details"].get(file_path)
    assert file_detail is not None and file_detail["status"] == "processed", ledger
    stdweb_id = file_detail["stdweb_id"]

    final_state = wait_for_task_state(
        stdweb_id, {"photometry_done", "failed"}
    )
    assert final_state == "photometry_done"

    # Cross-check against SkyPortal directly: the photometry point comment_watcher.py
    # reports as "uploaded" must actually be attached to the source, not merely
    # claimed by a bookkeeping bug in the reply message.
    response = requests.get(
        f"{SKYPORTAL_URL}/api/sources/{SOURCE_ID}/photometry", headers=SKYPORTAL_HEADERS
    )
    response.raise_for_status()
    photometry = response.json()["data"]
    matching = [p for p in photometry if p.get("origin") == f"stdweb:{stdweb_id}"]
    assert matching, f"No SkyPortal photometry point with origin stdweb:{stdweb_id}"
