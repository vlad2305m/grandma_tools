# Production setup

This service connects three already-running, independently-administered
systems: SkyPortal, StdWeb, and OwnCloud. Each needs one bot/service account
with a set of permissions.

If you're instead spinning up a fresh local/CI stack from scratch, see
`docker-compose.yaml` and `skyportal_processor_service_ci.yaml` -- that
path creates all accounts and registrations automatically. This doc is for
connecting to **existing** SkyPortal/StdWeb/OwnCloud deployments.

## 1. SkyPortal

Create a dedicated SkyPortal user (Admin → Users → New User, or
`POST /api/user`). Recommended: mark it `is_bot: true` and give it a
descriptive bio ("GRANDMA rescan service")

**ACLs**:

| ACL | Why |
|---|---|
| `Comment` | Posts the rescan-outcome reply on each source's thread. |
| `Run Analyses` | Reads/writes the per-source file ledger, stored as a SkyPortal Analysis entry (`GET`/`POST /api/obj/{id}/analysis*`). |
| `Manage Analysis Services` | Only needed once, to auto-create the `grandma_rescan` Analysis Service on first startup (see below). Can be revoked afterward. |

"Group admin" role has all the above ACLs, but is not recommended for
security reasons.

**Group membership**: the bot account must be a member of (or otherwise
have read access to) whatever SkyPortal group(s) the relevant sources
belong to.

**Analysis Service registration** (`grandma_rescan` / `grandma_rescan_trigger`, if no manage ACL):
on first startup, the service looks for an Analysis Service named
`grandma_rescan` (always needed) and, if `WEBHOOK_URL` is configured, 
a second one named `grandma_rescan_trigger` (optional, for "Run Analysis" button),
creating either if missing (needs `Manage Analysis Services`, above). If the
bot token doesn't have that ACL, startup fails with a `RuntimeError`
containing the JSON payload(s) to `POST` to `/api/analysis_service` --
an admin must run that once, then restart this service.

**API token**: generate a token for the bot user (Profile → Generate New
Token, or `POST /api/internal/tokens`). A token carries its own explicit
ACL subset -- make sure `Comment`, `Run Analyses`, and
`Manage Analysis Services` are all enabled for this token. Use the 
resulting token as `SKYPORTAL_TOKEN` in `.env`.

## 2. StdWeb

Create a dedicated StdWeb user.

**Permission**: `stdweb.skyportal_upload` ("Can upload the task results to
SkyPortal"). Grant via Django admin (Users → select user → Permissions) or:

**Group**: add the user to the group that should be able to see the tasks
and set `STDWEB_GROUP_ID` in `.env` to the group *name*.

**API token**: `python manage.py drf_create_token <username>`, or via
StdWeb's own UI if it exposes one. Use as `STDWEB_TOKEN` in `.env`.

## 3. OwnCloud: this service's own account

This service only reads from OwnCloud (`PROPFIND`/`GET`). 
The account it authenticates as only needs read access
to the folder tree under `SAVE_PATH`.

- Use an OwnCloud **app password** (Settings → Security → Create new app
  password) for `OWNCLOUD_TOKEN`.
- `OWNCLOUD_USER_ID` is whichever OwnCloud account's files tree the data
  lives under -- the WebDAV path is
  `/remote.php/dav/files/{OWNCLOUD_USER_ID}/...`. If the bot reads its own
  account's files, this is the same as `OWNCLOUD_USERNAME`.

## 4. This service's own `.env`

Copy `.env.default` and fill in the values from steps 1-3.

`TELESCOPE_INSTRUMENT_MAP` maps every OwnCloud telescope-folder name 
(`Candidates/Skyportal/<source>/<telescope>/file.fits`) that is 
different from the instrument name in SkyPortal.

`WEBHOOK_URL` is optional -- only needed for the "Run Analysis" button. 
(See "Triggering a rescan" in `README.md`)

## 5. Verifying the setup

1. Start the service (`python comment_watcher.py --env-file .env`) and
   confirm it logs `Listening for new sources...` without errors --
   startup itself only touches SkyPortal, exercising step 1 (token
   validity, `Manage Analysis Services` or a working manually-created
   `grandma_rescan` service). OwnCloud and StdWeb auth aren't exercised
   until the next step, when a real file actually gets scanned.
2. Post a plain (non-command) comment on a real source that has a file
   under a mapped telescope folder in OwnCloud. Within `POLL_INTERVAL`
   seconds, the service should pick it up, and within the StdWeb
   processing time, reply on the thread with `N new file(s) processed and
   uploaded to SkyPortal`.
3. Confirm the photometry point actually landed on the source
   (`GET /api/sources/{id}/photometry`, `origin: "stdweb:{task_id}"`) --
   this exercises step 3 (StdWeb's own upload).
4. Check the source's ledger (`GET /api/obj/{id}/analysis?analysisServiceID=<grandma_rescan id>`)
   shows the file as `processed` -- this exercises step 1's `Run Analyses`
   ACL and the Analysis Service registration together.
5. If `STDWEB_GROUP_ID` is set, log into StdWeb as a real operator account
   (a member of that group, not the bot) and open the StdWeb task link
   from the reply comment in step 2 -- it should load, not 403. An
   account that is *not* a member of that group should still get a 403.
