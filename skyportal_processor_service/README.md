# Comment processor README

An automatic monitoring service for SkyPortal comments that runs StdWeb forced
photometry on new files found under a source's OwnCloud folder, and uploads
the result back to the SkyPortal source as a photometry point.

## Features

* Polls SkyPortal for new comments (`comment_watcher.py`); any non-bot comment
  on a source triggers a check, no special command required.
* Recursively scans the source's OwnCloud folder for new FITS files.
* Drives StdWeb (create task, upload/point at the file, run `inspect` +
  `photometry`, poll to completion) for each new file found.
* Uploads the resulting forced photometry (mag/magerr/limiting magnitude,
  filter, mjd) to the SkyPortal source via StdWeb's
  `POST /api/tasks/{id}/skyportal/` endpoint, then replies on the comment
  thread with the outcome.
* Explicit `@rescanhelp` / `@rescandump` / `@rescanshow` commands for
  inspecting/debugging the in-flight job ledger.

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
