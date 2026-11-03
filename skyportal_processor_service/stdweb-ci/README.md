# StdWeb CI build files

`karpov-sv/stdweb` (the upstream repo CI builds for the integration test --
see `skyportal_processor_service_ci.yaml`) doesn't carry its own Dockerfile.
These three files are copied into the freshly-checked-out `stdweb/` directory
before `docker compose up --build` runs there, rather than living in (and
needing to be pushed to) the stdweb repo itself.

Keep these in sync with `stdweb/requirements.txt`/`stdweb/settings.py` if
upstream's dependencies or Celery/Redis wiring change.
