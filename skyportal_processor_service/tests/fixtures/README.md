# Test fixtures

## `test_image.fits`

Real CCD observation, not synthetic data, so that forced photometry in
`test_integration_comment_watcher.py` can actually calibrate against a live
reference catalog (StdWeb/stdpipe query VizieR for real stars at the image's
WCS position; a synthetic field has no real stars to match against).

- **Source**: [`karpov-sv/gmadet`](https://github.com/karpov-sv/gmadet),
  `gmadet/data_test/test_image.fits`, pinned at commit
  [`2695bcb`](https://github.com/karpov-sv/gmadet/blob/2695bcbc251fb33a7486971d4fb41ee86756441d/gmadet/data_test/test_image.fits).
- **License**: MIT (per the `gmadet` repository).
- **Content** (from the FITS header): a 2048x2048 `SDSS g` exposure of field
  `ATLAS18qqn` taken 2018-06-21 on telescope `ACP->NTM` / instrument `FLI`,
  with a valid tangent-plane WCS (`CRVAL1/2` = RA 244.000112892,
  Dec 22.2673593981, matching `FIXTURE_RA`/`FIXTURE_DEC` in
  `test_integration_comment_watcher.py`).

Previous fixture (replaced because its origin couldn't be traced back to a
documented public source) was at Dec -44.1°, which forced StdWeb's catalog
auto-selection to `skymapper` (`dec < -30`, see `stdweb`'s
`processing/inspect.py`). This fixture's Dec (+22.27°) instead auto-selects
`ps1` (Pan-STARRS DR1), avoiding a since-identified, unrelated third-party bug
in the `vizier.cfa.harvard.edu` SkyMapper DR4 mirror.
