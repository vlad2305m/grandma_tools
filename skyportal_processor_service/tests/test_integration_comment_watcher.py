import os
import shutil
import subprocess
import sys
import time
import uuid

import pytest
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv("../.env")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from comment_watcher import RESCAN_SERVICE_NAME  # noqa: E402

# OwnCloud's on-disk data directory, as bind-mounted by docker-compose.yaml --
# tests seed fixture files by writing here directly (see seed_owncloud_file
# below), never through OwnCloud's own write API.
OWNCLOUD_DATA_PATH = os.getenv("OWNCLOUD_DATA_PATH", "../data/owncloud")
OWNCLOUD_USER_ID = os.getenv("OWNCLOUD_USER_ID")
SAVE_PATH = os.getenv("SAVE_PATH")

SKYPORTAL_URL = os.getenv("SKYPORTAL_URL")
SKYPORTAL_TOKEN = os.getenv("SKYPORTAL_TOKEN")
SKYPORTAL_HEADERS = {"Authorization": f"token {SKYPORTAL_TOKEN}"}

STDWEB_URL = os.getenv("STDWEB_URL")
STDWEB_TOKEN = os.getenv("STDWEB_TOKEN")
STDWEB_HEADERS = {"Authorization": f"Token {STDWEB_TOKEN}"}

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "fixtures", "test_image.fits")
# Real WCS header of the fixture file -- must match, or forced photometry looks
# for the target at the wrong position.
FIXTURE_RA = 225.530009675195
FIXTURE_DEC = -44.1159352530986

SOURCE_ID = f"CIRESCANTEST{uuid.uuid4().hex[:8]}"
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
    # Standard ownCloud local-storage layout: {datadir}/{user}/files/{path}.
    dest = os.path.join(OWNCLOUD_DATA_PATH, OWNCLOUD_USER_ID, "files", relative_path)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copyfile(fixture_path, dest)

    subprocess.run(
        ["docker", "exec", "owncloud_server", "occ", "files:scan", OWNCLOUD_USER_ID],
        check=True,
    )


def post_comment_via_ui(source_id, text):
    """
    Post a comment as a real (session-authenticated, non-bot) SkyPortal user.

    SkyPortal marks *any* token-authenticated request as bot=True (see
    skyportal/handlers/api/comment.py: is_bot_request = isinstance(current_user,
    Token)), so comment_watcher.py's "any non-bot comment triggers a rescan"
    contract cannot be exercised via `requests` + SKYPORTAL_TOKEN at all -- it
    needs a real browser session. This drives one through SkyPortal's own
    debug-login OAuth stand-in (/login/google-oauth2/, backed by
    baselayer.app.psa.FakeGoogleOAuth2 when server.auth.debug_login is set,
    which it is by default) and fills the comment form exactly as a human
    would; SkyPortal's own Playwright test suite uses the same approach (see
    skyportal/tests/external/test_*.py).
    """
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(f"{SKYPORTAL_URL}/login/google-oauth2/", wait_until="networkidle")
        page.goto(f"{SKYPORTAL_URL}/source/{source_id}", wait_until="networkidle", timeout=60000)
        page.wait_for_selector("#root_comment", timeout=30000)
        page.fill("#root_comment", text)
        page.get_by_role("button", name="Add Comment").click()
        page.wait_for_timeout(1000)
        browser.close()


def get_comments():
    response = requests.get(
        f"{SKYPORTAL_URL}/api/sources/{SOURCE_ID}/comments",
        headers=SKYPORTAL_HEADERS,
    )
    response.raise_for_status()
    return response.json()["data"]


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
    End-to-end contract: a plain (non-command, non-bot) comment on a source
    with a new OwnCloud FITS file makes comment_watcher.py create+run a StdWeb
    forced-photometry task for it, upload the result to SkyPortal as a
    photometry point (via StdWeb's POST /api/tasks/{id}/skyportal/), and reply
    on the thread with the outcome -- see "any non-bot comment triggers a
    rescan" and stage 7 in comment_watcher.py.
    """
    before_ids = {c["id"] for c in get_comments()}
    post_comment_via_ui(SOURCE_ID, "please check for new data")
    new_comments = [c for c in get_comments() if c["id"] not in before_ids]
    assert new_comments, "UI comment did not appear via the API"
    trigger = new_comments[0]
    assert trigger["bot"] is False, "trigger comment must be non-bot to be picked up"
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
