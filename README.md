# zarr_vs_netcdf 
Continuous work in progress

## Comparing load and render speeds of netcdf  and zarr using xarray and SPEAR med output.

### Converts NetCDF files into two Zarr layouts optimized for:
  - Single-time global map reads
  - Full time-series point extraction

### Writes timing results to CSV.


## Structure
```bash
netcdf/                               # Input directory NetCDF files, can be symbolic links
zarr/                                 # Output directory generated Zarr stores
convert_nc_to_zarr_two_flavors.sh     # Conversion script
benchmark_nc_vs_zarr.sh               # Benchmark script
results.csv                           # Sample output
```

### 1. Usage: Convert NetCDF → Zarr
```bash
export NC_DIR="netcdf"
export ZARR_OUT="zarr"
export VAR_NAME="pr"

./convert_nc_to_zarr_two_flavors.sh "$NC_DIR" "$ZARR_OUT" "$VAR_NAME"
```

This creates:

- __map.zarr (map-optimized chunking)
- __ts.zarr (time-series-optimized chunking)

### Chunking Strategy

| Layout | Chunking                              | Optimized For    |
| ------ | ------------------------------------- | ---------------- |
| Map    | `{"time": 1, "lat": 90, "lon": 144}`  | Fast map slice   |
| TS     | `{"time": 120, "lat": 30, "lon": 32}` | Fast time series |


### 2. Run Benchmark

```bash
./benchmark_nc_vs_zarr.sh netcdf zarr pr 40.0 285.0 results.csv
```

Arguements:
```bash
benchmark_nc_vs_zarr.sh <NC_DIR> <ZARR_DIR> <VAR_NAME> <LAT> <LON> <OUTPUT_CSV>
```

Benchmarked workloads:

- Load one map (time slice)
- Plot one map
- Extract full time series at a point

