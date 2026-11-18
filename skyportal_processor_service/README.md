# Comment processor

An automatic monitoring service for SkyPortal comments that runs StdWeb forced
photometry on new files found under a source's OwnCloud folder, and uploads
the result back to the SkyPortal source as a photometry point.

## Features

* Polls SkyPortal for new comments (`comment_watcher.py`); any comment on
  a source triggers a check, except this service's own bot replies (a
  self-reply filter, not a blanket bot/non-bot one -- a different bot
  account's comment still counts, see `get_new_comments`) and an explicit
  `@rescanhelp`/`@rescandump` command, which reply directly instead of
  triggering a check (see below).
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

`tests/` additionally needs `pytest` and `pytest-xdist`.

## Configuration

See `.env.default` for the full list of environment variables, and
`SETUP.md` for what to register (bot accounts, permissions, the
`grandma_rescan`/`grandma_rescan_trigger` Analysis Services -- see "File
ledger" and "Triggering a rescan" below) on an existing
SkyPortal/StdWeb/OwnCloud deployment before pointing this service at it.
Notably:

* `TELESCOPE_INSTRUMENT_MAP` (optional, defaults to empty): JSON object
  mapping an OwnCloud telescope folder name (the path segment right after
  the source folder, e.g.
  `Candidates/Skyportal/<source>/<telescope>/file.fits`) to the exact
  SkyPortal instrument name to attribute its photometry to -- only needed
  when the two aren't byte-for-byte identical. A folder name that's
  already identical to a real instrument name resolves automatically (see
  `get_instrument_names`/`pipeline_tick`'s exact-match fallback in
  `comment_watcher.py`), deliberately with no normalization/fuzzy matching
  -- a telescope that's neither mapped here nor an exact instrument-name
  match fails the upload step outright (with a link to the StdWeb task so
  it can be uploaded manually) rather than silently attributing it to a
  generic/default instrument. Look up exact instrument names via
  `GET /api/instrument` on your SkyPortal instance.
* `STDWEB_TOKEN` needs to belong to a StdWeb user with the
  `stdweb.skyportal_upload` permission, and that StdWeb instance needs its own
  `SKYPORTAL_TOKEN`/`SKYPORTAL_BASE_URL` configured (StdWeb performs the
  SkyPortal upload itself, using its own credentials -- see
  `stdweb/REST_API.md`).

## Triggering a rescan

A separate `grandma_rescan_trigger` Analysis Service (distinct from
`grandma_rescan`, the ledger store described in "File ledger" below -- see
why in that section) can be made directly runnable from SkyPortal's own
UI/API -- a "Run Analysis" button on the source page, or
`POST /api/obj/<source_id>/analysis/<id>` -- without needing to post a
comment. This needs `comment_watcher.py` to accept an inbound connection,
which comments don't: set `WEBHOOK_URL` to this process's own
externally-reachable base URL (e.g. `http://100.90.108.127:8765`, reachable
from wherever SkyPortal itself runs) and it starts an HTTP listener on
`WEBHOOK_LISTEN_HOST`/`WEBHOOK_LISTEN_PORT` (`0.0.0.0:8765` by default) --
see `WebhookHandler`/`run_webhook_server` in `comment_watcher.py`. Leave
`WEBHOOK_URL` unset (the default): `grandma_rescan_trigger` is never
created, and the service stays comment-triggered only, exactly as before.

SkyPortal sends no authentication to this endpoint
(`authentication_type: "none"`) -- anyone who can reach
`WEBHOOK_LISTEN_HOST:WEBHOOK_LISTEN_PORT` can trigger a rescan of any real
source. `WebhookHandler.do_POST` only ever trusts the request's
`resource_id` as a lookup key, never any data in the body: `ra`/`dec` come
from its own authenticated `GET /api/sources/{id}`, not the request's
`inputs.obj` -- trusting the body there instead would let anyone who can
reach the port run forced photometry at an attacker-chosen position and
have it uploaded to SkyPortal under any real source's name. That leaves
triggering extra, unwanted rescan work as the actual exposure, not a
data-integrity risk (a rescan only ever acts on files already sitting in
OwnCloud) -- but firewall the port to SkyPortal's own network in
production rather than exposing it publicly.

## File ledger

Each source's processed/failed file ledger (which OwnCloud files have already
been turned into a StdWeb task and, if successful, uploaded to SkyPortal) is
stored as a SkyPortal [Analysis](https://skyportal.io/docs/analysis.html)
entry against the `grandma_rescan` service (status `completed`), not in the
comment thread -- plots/parameters/corner plots stay hidden (`show_*: false`)
-- see `restore_ledger`/`save_ledger` in
`comment_watcher.py`. This is always written the same way regardless of which
path (comment or the optional webhook trigger above) started the run:
`grandma_rescan` stays `upload_only: true` permanently, since SkyPortal's
`analysis_upload` endpoint (what a ledger save posts to) rejects anything
else -- the separate, not-`upload_only` `grandma_rescan_trigger` service
exists purely as that button's own entry point and UI feedback, and is never
read back from. Every ledger save prunes every other entry for that source,
so there's always at most one *completed* entry, and restoring on
restart/rescan just reads it back -- `restore_ledger` only ever considers
`completed` entries. This all assumes only one instance of this service runs
against a given SkyPortal instance at a time.

On first run against a SkyPortal instance, the service needs to find or
create an Analysis Service named `grandma_rescan` (always) and, if
`WEBHOOK_URL` is set, a second one named `grandma_rescan_trigger`. Finding
either only needs an ordinary token; **creating or updating** one needs the
"Manage Analysis Services" ACL, which a normal service token won't have (it
only needs "Run Analyses" for everything else this service does, including
uploading/reading/deleting ledger entries). If a service doesn't exist yet
and `SKYPORTAL_TOKEN` can't create it, startup fails with the exact JSON
payload an admin needs to `POST` once to `/api/analysis_service` (with their
own, more privileged token) -- after that, restart this service and it'll
find it by name every time. If `grandma_rescan_trigger` already exists but
`SKYPORTAL_TOKEN` can't update it to match the current `WEBHOOK_URL` config
(e.g. it's being turned on for the first time), this only logs a warning and
keeps using it as currently configured on SkyPortal -- ask an admin to toggle
`upload_only` and `display_on_resource_dropdown` there instead.

Set `GROUP_IDS` so ledger entries are visible to/readable by `SKYPORTAL_TOKEN`
regardless of its own personal group memberships: entries are explicitly
shared with those groups on upload. Without it, visibility falls back to
`SKYPORTAL_TOKEN`'s own accessible groups at upload time, which is fine for a
token that's an actual member of a relevant group but silently produces an
unreadable, orphaned ledger entry for a token with none (comment polling and
posting don't have this problem -- SkyPortal grants broader read access to
public/discoverable groups there than to arbitrary rows like an Analysis).
