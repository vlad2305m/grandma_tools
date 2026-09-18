import os
import sys
import time
import uuid

import pytest
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from requests.auth import HTTPBasicAuth

load_dotenv("../.env")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from comment_watcher import job_from_comment  # noqa: E402

OWNCLOUD_BASE_URL = os.getenv("OWNCLOUD_BASE_URL")
OWNCLOUD_USERNAME = os.getenv("OWNCLOUD_USERNAME")
OWNCLOUD_TOKEN = os.getenv("OWNCLOUD_TOKEN")
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
OWNCLOUD_AUTH = HTTPBasicAuth(OWNCLOUD_USERNAME, OWNCLOUD_TOKEN)


def owncloud_mkcol(path):
    requests.request(
        "MKCOL",
        f"{OWNCLOUD_BASE_URL}/remote.php/dav/files/{OWNCLOUD_USER_ID}/{path}",
        auth=OWNCLOUD_AUTH,
    )


def owncloud_put_file(path, content):
    response = requests.put(
        f"{OWNCLOUD_BASE_URL}/remote.php/dav/files/{OWNCLOUD_USER_ID}/{path}",
        data=content,
        auth=OWNCLOUD_AUTH,
    )
    response.raise_for_status()


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

    owncloud_mkcol(f"{SAVE_PATH}/{SOURCE_ID}")
    owncloud_mkcol(f"{SAVE_PATH}/{SOURCE_ID}/{TELESCOPE_FOLDER}")
    with open(FIXTURE_PATH, "rb") as f:
        owncloud_put_file(
            f"{SAVE_PATH}/{SOURCE_ID}/{TELESCOPE_FOLDER}/test_image.fits", f.read()
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

    job = job_from_comment(reply["text"])
    assert job is not None, f"Could not decode job ledger from reply: {reply['text']!r}"
    assert len(job.file_jobs) == 1, job.file_jobs
    file_job = job.file_jobs[0]
    assert file_job.stage == 1000, f"File job did not reach done stage: {file_job}"

    final_state = wait_for_task_state(
        file_job.stdweb_id, {"photometry_done", "failed"}
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
    matching = [p for p in photometry if p.get("origin") == f"stdweb:{file_job.stdweb_id}"]
    assert matching, f"No SkyPortal photometry point with origin stdweb:{file_job.stdweb_id}"
