#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./benchmark_nc_vs_zarr.sh <NC_INPUT_DIR> <ZARR_OUTPUT_DIR> [VAR_NAME] [LAT] [LON] [CSV_OUT]
#
# Example:
#   ./benchmark_nc_vs_zarr.sh netcdf zarr pr 40.0 285.0 results.csv
#
# Optional env overrides:
#   REPEAT=10 WARMUP=2 ./benchmark_nc_vs_zarr.sh netcdf zarr pr 40 285 results.csv

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <NC_INPUT_DIR> <ZARR_OUTPUT_DIR> [VAR_NAME] [LAT] [LON] [CSV_OUT]"
  exit 1
fi

NC_DIR="$1"
ZARR_OUT="$2"
VAR_NAME="${3:-pr}"
LAT_PT="${4:-40.0}"
LON_PT="${5:-285.0}"
CSV_OUT="${6:-benchmark_results.csv}"

REPEAT="${REPEAT:-5}"
WARMUP="${WARMUP:-1}"

python - "$NC_DIR" "$ZARR_OUT" "$VAR_NAME" "$LAT_PT" "$LON_PT" "$CSV_OUT" "$REPEAT" "$WARMUP" <<'PY'
import sys, time
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

nc_dir = Path(sys.argv[1])
zarr_out = Path(sys.argv[2])
var_name = sys.argv[3]
lat_pt = float(sys.argv[4])
lon_pt = float(sys.argv[5])
csv_out = Path(sys.argv[6])
repeat = int(sys.argv[7])
warmup = int(sys.argv[8])

def bench(fn, repeat=5, warmup=1):
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    times = np.array(times, dtype=float)
    return dict(
        mean_s=float(times.mean()),
        median_s=float(np.median(times)),
        min_s=float(times.min()),
        std_s=float(times.std(ddof=1)) if len(times) > 1 else 0.0,
    )

def render_map(da):
    fig, ax = plt.subplots(figsize=(7,3))
    da.plot(ax=ax)
    fig.canvas.draw()
    plt.close(fig)

rows = []

nc_files = sorted(nc_dir.glob("*.nc"))
if not nc_files:
    raise SystemExit(f"No .nc files found in {nc_dir}")

for nc in nc_files:
    stem = nc.stem
    z_map = zarr_out / f"{stem}__map.zarr"
    z_ts  = zarr_out / f"{stem}__ts.zarr"

    if not z_map.exists() or not z_ts.exists():
        print(f"Skipping {nc.name}: missing {z_map.name} or {z_ts.name}")
        continue

    print(f"\n=== Benchmarking {nc.name} ===")

    # A) Load 1 global map
    def load_map_nc():
        ds = xr.open_dataset(nc, engine="netcdf4", cache=False)[[var_name]]
        ds[var_name].isel(time=0).load()

    def load_map_zmap():
        ds = xr.open_zarr(z_map, consolidated=True)[[var_name]]
        ds[var_name].isel(time=0).load()

    s_nc = bench(load_map_nc, repeat=repeat, warmup=warmup)
    s_za = bench(load_map_zmap, repeat=repeat, warmup=warmup)
    rows.append({
        "file": nc.name, "workload": "load_1_map",
        "netcdf_mean_s": s_nc["mean_s"], "netcdf_median_s": s_nc["median_s"],
        "zarr_mean_s": s_za["mean_s"], "zarr_median_s": s_za["median_s"],
        "speedup_x": s_nc["mean_s"]/s_za["mean_s"],
        "zarr_pct_faster": (1 - (s_za["mean_s"]/s_nc["mean_s"])) * 100,
        "zarr_store": z_map.name
    })

    # B) Plot 1 global map (load + render)
    def plot_map_nc():
        ds = xr.open_dataset(nc, engine="netcdf4", cache=False)[[var_name]]
        da = ds[var_name].isel(time=0).load()
        render_map(da)

    def plot_map_zmap():
        ds = xr.open_zarr(z_map, consolidated=True)[[var_name]]
        da = ds[var_name].isel(time=0).load()
        render_map(da)

    s_nc = bench(plot_map_nc, repeat=repeat, warmup=warmup)
    s_za = bench(plot_map_zmap, repeat=repeat, warmup=warmup)
    rows.append({
        "file": nc.name, "workload": "plot_1_map",
        "netcdf_mean_s": s_nc["mean_s"], "netcdf_median_s": s_nc["median_s"],
        "zarr_mean_s": s_za["mean_s"], "zarr_median_s": s_za["median_s"],
        "speedup_x": s_nc["mean_s"]/s_za["mean_s"],
        "zarr_pct_faster": (1 - (s_za["mean_s"]/s_nc["mean_s"])) * 100,
        "zarr_store": z_map.name
    })

    # C) Point time series (full record)
    def point_ts_nc():
        ds = xr.open_dataset(nc, engine="netcdf4", cache=False)[[var_name]]
        ds[var_name].sel(lat=lat_pt, lon=lon_pt, method="nearest").load()

    def point_ts_zts():
        ds = xr.open_zarr(z_ts, consolidated=True)[[var_name]]
        ds[var_name].sel(lat=lat_pt, lon=lon_pt, method="nearest").load()

    s_nc = bench(point_ts_nc, repeat=repeat, warmup=warmup)
    s_za = bench(point_ts_zts, repeat=repeat, warmup=warmup)
    rows.append({
        "file": nc.name, "workload": "point_ts_full",
        "netcdf_mean_s": s_nc["mean_s"], "netcdf_median_s": s_nc["median_s"],
        "zarr_mean_s": s_za["mean_s"], "zarr_median_s": s_za["median_s"],
        "speedup_x": s_nc["mean_s"]/s_za["mean_s"],
        "zarr_pct_faster": (1 - (s_za["mean_s"]/s_nc["mean_s"])) * 100,
        "zarr_store": z_ts.name
    })

df = pd.DataFrame(rows)
df.to_csv(csv_out, index=False)

print(f"\nWrote: {csv_out.resolve()}")
print(df)
PY
