# StdWeb CI build files

`karpov-sv/stdweb` (the upstream repo CI builds for the integration test --
see `skyportal_processor_service_ci.yaml`) doesn't carry its own Dockerfile.
`Dockerfile`, `docker-entrypoint.sh`, and `.dockerignore` are copied into the
freshly-checked-out `stdweb/` directory before it gets built, rather than
living in (and needing to be pushed to) the stdweb repo itself.

Keep these in sync with `stdweb/requirements.txt`/`stdweb/settings.py` if
upstream's dependencies or Celery/Redis wiring change.

Note: previously also patched in an `STDWEB_LOCAL_CATALOG_FILE` escape
hatch (`local_catalog.patch`) to bypass a live VizieR query in the
integration test, back when both VizieR and its documented mirror
fallbacks were unreliable from CI. No longer needed -- upstream stdpipe
added its own non-VizieR fallback (MAST's PanSTARRS API) that reliably
picks up where the VizieR mirrors still fail from this network; see
`tests/fixtures/README.md`'s git history for what this replaced.
