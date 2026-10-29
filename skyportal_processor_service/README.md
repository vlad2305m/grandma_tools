# Comment processor README

An automatic monitoring service for SkyPortal comments that runs StdWeb forced
photometry on new files found under a source's OwnCloud folder, and uploads
the result back to the SkyPortal source as a photometry point.

## Features

* Polls SkyPortal for new comments (`comment_watcher.py`); any non-bot comment
  on a source triggers a check, no special command required.
* Also runnable directly from SkyPortal's own "Run Analysis" UI/API, without
  posting a comment -- see "Triggering a rescan" below.
* Recursively scans the source's OwnCloud folder for new FITS files, checking
  each one against a per-source processed/failed file ledger (see "File
  ledger" below) so already-handled files are never redone.
* Drives StdWeb (create task, upload/point at the file, run `inspect` +
  `photometry`, poll to completion) for each new file found.
* Uploads the resulting forced photometry (mag/magerr/limiting magnitude,
  filter, mjd) to the SkyPortal source via StdWeb's
  `POST /api/tasks/{id}/skyportal/` endpoint, then replies on the comment
  thread with the outcome.
* Explicit `@rescanhelp` / `@rescandump` commands for inspecting/debugging
  the in-flight job pipeline.

## Dependencies

Tool dependencies are available in the `requirements.txt` file.
Required packages:
* `requests`
* `python-dotenv`

`tests/` additionally needs `pytest` and `playwright` (with a Chromium
browser: `python -m playwright install chromium`) -- the integration test
drives a real browser to post comments, since SkyPortal marks any
token-authenticated request as bot, which `comment_watcher.py` ignores.

## Configuration

See `.env.default` for the full list of environment variables. Notably:

* `TELESCOPE_INSTRUMENT_MAP` (required): JSON object mapping each OwnCloud
  telescope folder name (the path segment right after the source folder,
  e.g. `Candidates/Skyportal/<source>/<telescope>/file.fits`) to the exact
  SkyPortal instrument name to attribute its photometry to. Every telescope
  folder that can appear under `SAVE_PATH` must have an entry -- a file under
  an unmapped telescope fails the upload step outright (with a link to the
  StdWeb task so it can be uploaded manually) rather than silently attributing
  it to a generic/default instrument. Look up exact instrument names via
  `GET /api/instrument` on your SkyPortal instance.
* `STDWEB_TOKEN` needs to belong to a StdWeb user with the
  `stdweb.skyportal_upload` permission, and that StdWeb instance needs its own
  `SKYPORTAL_TOKEN`/`SKYPORTAL_BASE_URL` configured (StdWeb performs the
  SkyPortal upload itself, using its own credentials -- see
  `stdweb/REST_API.md`).

## Triggering a rescan

The same `grandma_rescan` Analysis Service (see "File ledger" below) can also
be made directly runnable from SkyPortal's own UI/API -- a "Run Analysis"
button on the source page, or `POST /api/obj/<source_id>/analysis/<id>` --
without needing to post a comment. This needs `comment_watcher.py` to accept
an inbound connection, which comments don't: set `WEBHOOK_URL` to this
process's own externally-reachable base URL (e.g.
`http://100.90.108.127:8765`, reachable from wherever SkyPortal itself runs)
and it starts an HTTP listener on `WEBHOOK_LISTEN_HOST`/`WEBHOOK_LISTEN_PORT`
(`0.0.0.0:8765` by default) -- see `WebhookHandler`/`run_webhook_server` in
`comment_watcher.py`. Leave `WEBHOOK_URL` unset (the default) to keep the
service comment-triggered only, exactly as before; nothing else changes.

SkyPortal sends no authentication to this endpoint
(`authentication_type: "none"`) -- anyone who can reach
`WEBHOOK_LISTEN_HOST:WEBHOOK_LISTEN_PORT` can trigger a rescan of any real
source. That's a nuisance, not a data-integrity risk (a rescan only ever acts
on files already sitting in OwnCloud), but firewall the port to SkyPortal's
own network in production rather than exposing it publicly.

## File ledger

Each source's processed/failed file ledger (which OwnCloud files have already
been turned into a StdWeb task and, if successful, uploaded to SkyPortal) is
stored as a SkyPortal [Analysis](https://skyportal.io/docs/analysis.html)
entry against the same `grandma_rescan` service used for triggering (status
`completed`), not in the comment thread -- plots/parameters/corner plots stay
hidden (`show_*: false`) either way, so it never clutters the source page
even when webhook triggering is on and the entry does show up there --
see `restore_ledger`/`save_ledger`/`send_webhook_callback` in
`comment_watcher.py`. Every ledger save (or completed webhook-triggered run)
prunes every other entry for that source, so there's always at most one
*completed* entry, and restoring on restart/rescan just reads it back --
`restore_ledger` only ever considers `completed` entries, so an in-flight or
failed webhook-triggered run never looks like "this source has no ledger
yet". This all assumes only one instance of this service runs against a
given SkyPortal instance at a time.

On first run against a SkyPortal instance, the service needs to find or
create an Analysis Service named `grandma_rescan`. Finding it only needs an
ordinary token; **creating or updating** it needs the "Manage Analysis
Services" ACL, which a normal service token won't have (it only needs "Run
Analyses" for everything else this service does, including
uploading/reading/deleting ledger entries). If the service doesn't exist yet
and `SKYPORTAL_TOKEN` can't create it, startup fails with the exact JSON
payload an admin needs to `POST` once to `/api/analysis_service` (with their
own, more privileged token) -- after that, restart this service and it'll
find it by name every time. If it already exists but `WEBHOOK_URL`'s
presence/absence no longer matches how it's configured there (e.g. webhook
triggering is being turned on for the first time) and `SKYPORTAL_TOKEN` can't
update it either, this only logs a warning and keeps using it as currently
configured on SkyPortal -- ask an admin to toggle `upload_only` and
`display_on_resource_dropdown` there instead.

Set `GROUP_IDS` so ledger entries are visible to/readable by `SKYPORTAL_TOKEN`
regardless of its own personal group memberships: entries are explicitly
shared with those groups on upload. Without it, visibility falls back to
`SKYPORTAL_TOKEN`'s own accessible groups at upload time, which is fine for a
token that's an actual member of a relevant group but silently produces an
unreadable, orphaned ledger entry for a token with none (comment polling and
posting don't have this problem -- SkyPortal grants broader read access to
public/discoverable groups there than to arbitrary rows like an Analysis).

## Testing policy: OwnCloud is read-only, always

`comment_watcher.py` never writes to OwnCloud (only `PROPFIND`/`GET`) -- it's
strictly a consumer of files other tools put there. Tests follow the same
rule, including against the disposable Docker sandbox in
`docker-compose.yaml`: `tests/test_integration_comment_watcher.py` seeds its
fixture file by writing directly to OwnCloud's bind-mounted data directory
(`OWNCLOUD_DATA_PATH`, see `.env.default`) and triggering a local `occ
files:scan` via `docker exec`, never a WebDAV `PUT`/`MKCOL`/`DELETE`. When
testing manually against a real (non-Docker) OwnCloud instance, this means
using files that already exist there -- never create or upload new ones for
a test, since there is no "local storage" to write to directly on a remote
instance.
