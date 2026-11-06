# StdWeb CI build files

`karpov-sv/stdweb` (the upstream repo CI builds for the integration test --
see `skyportal_processor_service_ci.yaml`) doesn't carry its own Dockerfile.
`Dockerfile`, `docker-entrypoint.sh`, and `.dockerignore` are copied into the
freshly-checked-out `stdweb/` directory before it gets built, rather than
living in (and needing to be pushed to) the stdweb repo itself.

Keep these in sync with `stdweb/requirements.txt`/`stdweb/settings.py` if
upstream's dependencies or Celery/Redis wiring change.

`local_catalog.patch` is applied with `git apply` (not copied) to
`stdweb/processing/photometry.py` right after checkout. It adds an
`STDWEB_LOCAL_CATALOG_FILE` environment-variable escape hatch: when set, the
forced-photometry step reads a pre-fetched catalogue file from disk instead
of querying VizieR live, letting the integration test (see
`tests/fixtures/ps1_cat.parquet`) run without depending on a flaky live
third-party service. Unset in production, so production behavior is
unchanged -- see the "Reference catalogue" patch context for where it hooks
in. Regenerate against upstream's current `photometry.py` if this stops
applying cleanly (upstream changes around that function occasionally).
