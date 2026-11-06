# Production setup

This service connects three already-running, independently-administered
systems: SkyPortal, StdWeb, and OwnCloud. None of them need to be
redeployed or reconfigured wholesale -- each needs one bot/service account
with a specific, minimal set of permissions. This doc covers what to
register where, for each system, before pointing `comment_watcher.py` at
real instances.

If you're instead spinning up a fresh local/CI stack from scratch, see
`docker-compose.yaml` and `skyportal_processor_service_ci.yaml` -- that
path creates all accounts and registrations automatically. This doc is for
connecting to **existing** SkyPortal/StdWeb/OwnCloud deployments.

There are two separate bot accounts to create: one on SkyPortal for this
service itself, and one on StdWeb for it to drive forced photometry. StdWeb
also needs its *own*, third, SkyPortal account/token, configured on the
StdWeb side -- it uploads photometry directly, not through this service.
Three accounts total, each with only the permissions it actually uses.

## 1. SkyPortal: this service's own bot account

Create a dedicated SkyPortal user (Admin → Users → New User, or
`POST /api/user`). Recommended: mark it `is_bot: true` and give it a
descriptive bio ("GRANDMA rescan service") -- this is just SkyPortal's own
bookkeeping flag and doesn't affect this service's own self-reply filter
(that's based on user id, not this flag -- see `SELF_USER_ID` in
`comment_watcher.py`).

**ACLs** (grant directly to the user -- `User.acls`, separate from role
membership, so you don't have to hand out an entire role bundle like
"Group admin" just for these two or three permissions):

| ACL | Why |
|---|---|
| `Comment` | Posts the rescan-outcome reply on each source's thread. |
| `Run Analyses` | Reads/writes the per-source file ledger, stored as a SkyPortal Analysis entry (`GET`/`POST /api/obj/{id}/analysis*`). |
| `Manage Analysis Services` | Only needed once, to auto-create the `grandma_rescan` Analysis Service on first startup (see below). Can be revoked afterward if you'd rather not leave it granted long-term. |

None of SkyPortal's built-in roles match this exactly: "Full user" has
`Comment` + `Run Analyses` but not `Manage Analysis Services`; "Group
admin" has all three but also unrelated admin powers (manage users, manage
sources, manage teams, ...). Granting the three ACLs directly to the bot
user, rather than assigning a role, keeps it to exactly what's used.

**Group membership**: the bot account must be a member of (or otherwise
have read access to) whatever SkyPortal group(s) the relevant sources
belong to -- it can't see or comment on sources it has no access to. Set
`GROUP_IDS` in `.env` to those same group ids; Analysis-entry (ledger)
uploads are explicitly shared with these groups, so ledger entries stay
readable by the bot even if its own personal group memberships change
later.

**Analysis Service registration** (`grandma_rescan`): on first startup,
the service looks for an Analysis Service named `grandma_rescan` and
creates it if missing (needs `Manage Analysis Services`, above). If the
bot token doesn't have that ACL, startup fails with a `RuntimeError`
containing the exact JSON payload to `POST` to `/api/analysis_service` --
have an admin run that once, with their own more-privileged token, then
restart this service; it finds the service by name from then on. This is
the same Analysis Service that backs both the per-source file ledger and
(if `WEBHOOK_URL` is configured) the optional "Run Analysis" trigger
button -- see "Triggering a rescan" in `README.md`.

**API token**: generate a token for the bot user (Profile → Generate New
Token, or `POST /api/internal/tokens`). A token carries its own explicit
ACL subset, checked against (but not automatically inherited from) the
user's ACLs -- make sure `Comment`, `Run Analyses`, and
`Manage Analysis Services` are all explicitly selected/included for this
token specifically, not just granted on the user. Use the resulting token
as `SKYPORTAL_TOKEN` in `.env`.

## 2. StdWeb: this service's own bot account

Create a dedicated StdWeb user (Django admin, or `manage.py createsuperuser`
if you want it to also manage StdWeb itself -- not required).

**Permission**: `stdweb.skyportal_upload` ("Can upload the task results to
SkyPortal") -- a standard Django model permission on `stdweb.Task`. Grant
it via Django admin (Users → select user → Permissions) or:

```python
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
user = get_user_model().objects.get(username="grandma-rescan")
user.user_permissions.add(Permission.objects.get(codename="skyportal_upload"))
```

No other StdWeb permission is needed: this service only ever creates,
uploads to, and reads back its *own* tasks, and task ownership alone is
sufficient to create/run/view them (`stdweb.view_all_tasks`/
`stdweb.edit_all_tasks` are for working with *other* users' tasks, not
needed here).

**API token**: `python manage.py drf_create_token <username>`, or via
StdWeb's own UI if it exposes one. Use this as `STDWEB_TOKEN` in `.env`.

## 3. StdWeb's own SkyPortal credentials (not this service's)

StdWeb makes its own direct `PUT /api/photometry` call to upload the
forced-photometry result -- a separate request from anything this service
sends, using StdWeb's *own* SkyPortal token, not `SKYPORTAL_TOKEN` above.
This is configured on the StdWeb deployment itself (its `settings.py`
`config()` calls, or `SKYPORTAL_TOKEN`/`SKYPORTAL_BASE_URL` in whatever env
file that instance reads), not in this service's `.env`.

Create a third SkyPortal account (or reuse the bot account from step 1 --
a separate one is clearer about which system holds which privilege) with
the **`Upload data`** ACL, and make sure the token generated for it
explicitly includes that ACL too (tokens carry their own explicit ACL
subset -- see the "API token" note in step 1). `Comment`/`Run Analyses`
from step 1 don't cover this; it's a different endpoint.

If this is missing or misconfigured, every task's final upload step fails
with `"SKYPORTAL_TOKEN is not configured on this STDWeb instance"` or a
403 from SkyPortal -- check StdWeb's own logs, not this service's.

## 4. OwnCloud: this service's own account

This service only ever reads from OwnCloud (`PROPFIND`/`GET`) -- it has no
write path at all, in production or in tests (see "Testing policy" in
`README.md`). The account it authenticates as only needs **read access**
to the folder tree under `SAVE_PATH`.

- Use an OwnCloud **app password** (Settings → Security → Create new app
  password), not the account's real login password, for `OWNCLOUD_TOKEN`.
  Authentication is plain HTTP Basic Auth (`OWNCLOUD_USERNAME`/
  `OWNCLOUD_TOKEN`) against WebDAV.
- `OWNCLOUD_USER_ID` is whichever OwnCloud account's files tree the data
  actually lives under -- the WebDAV path is
  `/remote.php/dav/files/{OWNCLOUD_USER_ID}/...`. If the bot reads its own
  account's files, this is the same as `OWNCLOUD_USERNAME`. If it instead
  reads a folder shared *to* the bot from another account (common if
  telescope pipelines upload under their own accounts), `OWNCLOUD_USER_ID`
  is that other account's username, not the bot's.

## 5. This service's own `.env`

Copy `.env.default` and fill in the values from steps 1-2 and 4:

| Variable | From |
|---|---|
| `OWNCLOUD_BASE_URL`, `OWNCLOUD_USERNAME`, `OWNCLOUD_TOKEN`, `OWNCLOUD_USER_ID` | Step 4 |
| `SKYPORTAL_URL`, `SKYPORTAL_TOKEN` | Step 1 |
| `STDWEB_URL`, `STDWEB_TOKEN` | Step 2 |
| `GROUP_IDS` | Step 1 (group membership) |
| `TELESCOPE_INSTRUMENT_MAP` | See below |

`TELESCOPE_INSTRUMENT_MAP` maps every OwnCloud telescope-folder name that
can appear under `SAVE_PATH` (the path segment right after the source
folder, e.g. `Candidates/Skyportal/<source>/<telescope>/file.fits`) to the
exact SkyPortal instrument name to attribute its photometry to. This has
to be exhaustive: a file under an unmapped telescope folder fails the
upload step outright (with a link to the StdWeb task so it can be uploaded
manually) rather than silently attributing it to some default instrument.
Look up exact instrument names with `GET /api/instrument` on your
SkyPortal instance (`token $SKYPORTAL_TOKEN` from step 1 works for this).

`WEBHOOK_URL` is optional -- only needed for the "Run Analysis" button
trigger path, not for comment-triggered rescans. See "Triggering a
rescan" in `README.md` if you want that.

## 6. Verifying the setup

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
