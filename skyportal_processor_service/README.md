# Comment processor

An automatic monitoring service for SkyPortal comments that runs StdWeb forced
photometry on new files found under a source's OwnCloud folder, and uploads
the result back to the SkyPortal source as a photometry point.

## Features

* Polls SkyPortal for new comments (`comment_watcher.py`); any comment with 
  @rescan triggers a check.
* Also runnable directly from SkyPortal's own "Run Analysis" UI/API, without
  posting a comment -- see "Triggering a rescan" below.
* Recursively scans the source's OwnCloud folder for new FITS files, checking
  each one against a per-source processed/failed file ledger (see "File
  ledger" below) so already-handled files are never redone.
* Runs StdWeb (create task, upload/point at the file, run `inspect` +
  `photometry`, poll to completion) for each new file found.
* Uploads the resulting forced photometry (mag/magerr/limiting magnitude,
  filter, mjd) to the SkyPortal source via StdWeb's
  `POST /api/tasks/{id}/skyportal/` endpoint, then replies on the comment
  thread with the outcome.
* `@rescanhelp` / `@rescandump` commands for inspecting/debugging
  the in-flight job pipeline.

## Dependencies

See `requirements.txt` file.
Required packages:
* `requests`
* `python-dotenv`

`tests/` additionally needs `pytest` and `pytest-xdist`.

## Configuration

See `.env.default` for the list of settings, and
`SETUP.md` for instructions.

## File ledger

Analysis Service named `grandma_rescan` is used to store state
(processed files, errors, ...) (separately for each source).
The state is updated after a rescan job finishes.

## Triggering a rescan

Rescanning may be triggered via a "Run Analysis" button in the
SkyPortal UI if a port of this service is exposed to SkyPortal.
A separate `grandma_rescan_trigger` Analysis Service will be
registered to that port and enabled if `WEBHOOK_URL` is set.

Restricting external access to the exposed port via a firewall 
is strongly advised.
