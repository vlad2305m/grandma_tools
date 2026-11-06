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

## `ps1_cat.parquet`

Pre-fetched reference catalogue for `test_image.fits`'s field, used via the
`STDWEB_LOCAL_CATALOG_FILE` escape hatch (see `stdweb-ci/local_catalog.patch`
and `stdweb-ci/README.md`) so the integration test doesn't depend on a live
VizieR query -- both the primary VizieR server and its documented fallback
mirrors have proven unreliable from CI (persistent connection timeouts on
`vizier.cds.unistra.fr`, and a server-side backend crash for multiple
catalogues on `vizier.cfa.harvard.edu`/`vizier.idia.ac.za`), independent of
which catalogue is requested.

- **Source**: [MAST PanSTARRS DR2 mean-object API](https://catalogs.mast.stsci.edu/docs/panstarrs.html),
  queried for a 0.3° cone around `test_image.fits`'s field center, filtered to
  1215 stars with clean g/r/i/z/y PSF photometry.
- **Schema**: raw MAST columns renamed to VizieR's `II/349/ps1` convention
  (`RAJ2000`/`DEJ2000`/`gmag`/`e_gmag`/etc.), then run through stdpipe's own
  `catalogs.augment_cat_bands(cat, catalog='ps1')` to add the derived
  Johnson-Cousins columns -- byte-for-byte the same post-processing a live
  `get_cat_vizier('ps1', ...)` call would apply, so the file is a drop-in
  substitute for the live query's output.
- **License**: MAST/STScI catalog data is public domain (US government-funded
  survey data).
