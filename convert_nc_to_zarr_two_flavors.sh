#!/usr/bin/env bash
set -euo pipefail

# Convert all NetCDF files in a directory to TWO Zarr layouts:
#  - map-chunked: time=1,  lat=90, lon=144
#  - ts-chunked : time=120, lat=30, lon=32
#
# Usage:
#   ./convert_nc_to_zarr_two_flavors.sh <NC_INPUT_DIR> <ZARR_OUTPUT_DIR> [VAR_NAME]
#
# Example:
#   ./convert_nc_to_zarr_two_flavors.sh netcdf zarr pr

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <NC_INPUT_DIR> <ZARR_OUTPUT_DIR> [VAR_NAME]"
  exit 1
fi

NC_DIR="$1"
ZARR_OUT="$2"
VAR_NAME="${3:-pr}"

python - "$NC_DIR" "$ZARR_OUT" "$VAR_NAME" <<'PY'
import sys, shutil, warnings
from pathlib import Path

import xarray as xr
from dask.diagnostics import ProgressBar
from numcodecs import Blosc

nc_dir = Path(sys.argv[1])
zarr_out = Path(sys.argv[2])
var_name = sys.argv[3]

zarr_out.mkdir(parents=True, exist_ok=True)

# Two flavors of chunking ("optimiszing" for plotting vs for timeseires use cases)
CHUNK_MAP = {"time": 1, "lat": 90, "lon": 144}
CHUNK_TS  = {"time": 120, "lat": 30, "lon": 32}

#Zarr-v2-compatible compressor ??
compressor = Blosc(cname="zstd", clevel=3, shuffle=Blosc.SHUFFLE)
encoding = {var_name: {"compressor": compressor}}

def write_zarr(ds: xr.Dataset, out_path: Path):
    if out_path.exists():
        shutil.rmtree(out_path)
    with ProgressBar():
        ds.to_zarr(out_path, mode="w", consolidated=True, encoding=encoding)

nc_files = sorted(nc_dir.glob("*.nc"))
if not nc_files:
    raise SystemExit(f"No .nc files found in {nc_dir}")

# Silence the “specified chunks separate stored chunks” warning
warnings.filterwarnings(
    "ignore",
    message="The specified chunks separate the stored chunks",
    category=UserWarning,
)

for nc_file in nc_files:
    stem = nc_file.stem
    z_map = zarr_out / f"{stem}__map.zarr"
    z_ts  = zarr_out / f"{stem}__ts.zarr"

    print(f"\n=== {nc_file.name} ===")

    # Open without chunks to avoid chunk-splitting warnings upon opening the file, then rechunk in dask before writing.
    ds0 = xr.open_dataset(nc_file, engine="netcdf4")[[var_name]]

    print("Writing:", z_map)
    write_zarr(ds0.chunk(CHUNK_MAP), z_map)

    print("Writing:", z_ts)
    write_zarr(ds0.chunk(CHUNK_TS), z_ts)

print("\nDone.")
PY

