# Resources

This example reads its static inputs from this directory.

Required:

- `austria-east.geojson`: AOI polygon used for download metadata, reprojection, clipping, and reporting

Optional preprocessing inputs:

- `processed-inputs/`: post-event VV/VH rasters and optional `local_incidence_angle.tif`
- `pre-event/`: pre-event VV/VH rasters for change-based flood detection

Optional post-processing overlays:

- `permanent-water.geojson`
- `roads.geojson`
- `settlements.geojson`
- `agriculture.geojson`

All vector resources are expected to be GeoJSON in `EPSG:4326`.
All raster resources should already be geocoded and readable by `rasterio`.
