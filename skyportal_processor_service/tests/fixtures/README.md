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
`ps1` (Pan-STARRS DR1).

Note: this directory previously also carried a pre-fetched `ps1_cat.parquet`
catalogue, used to bypass a live VizieR query that was unreliable from CI
(both the primary server and its documented mirror fallbacks). Removed --
upstream stdpipe added its own non-VizieR fallback (MAST's PanSTARRS API)
that reliably succeeds where the VizieR mirrors still fail from this
network; see git history if that ever needs reviving.
