import earthaccess
import xarray as xr
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive plotting for saving figures
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import ssl
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
auth = earthaccess.login()
short_name = "PACE_SPEXONE_L2_AER_RTAPLAND"  # Change this if a different short name is used
start_date = "2024-04"
end_date = "2025-04"

# results = earthaccess.search_data(
results = earthaccess.search_data(
    short_name=short_name,
    temporal=(start_date,end_date),
    # granule_name="*.DAY.*0p1deg*",  # Daily only for MOANA | Resolution: 0p1deg or 4 (for 4km)

)
len(results)
# paths = earthaccess.open(results[0])

#fvf has no wavelength dimension 
variables = [ 'ssa', 'fmf','mr','mi','aot','lidar_depol_ratio']

parquet_output = f'~/Desktop/Extract_{short_name}_{start_date}_{end_date}.parquet'

variables = ['ssa', 'fmf', 'mr', 'mi', 'aot', 'lidar_depol_ratio']
variables_2d = []  # These have only 2D data (lat, lon)
pqwriter = None
if os.path.exists(parquet_output):
    os.remove(parquet_output)
pqwriter = None

for f in spex_files:
    try:
        datatree = xr.open_datatree(f)
        ds = xr.merge(datatree.to_dict().values())

        time_str = ds.attrs.get("time_coverage_start")
        if not time_str:
            print(f"Skipping {f}: Missing time_coverage_start")
            continue

        obs_time = pd.to_datetime(time_str)

        # Latitude and longitude (2D)
        lat_vals = ds['latitude'].values
        lon_vals = ds['longitude'].values

        if 'aot' not in ds or 'wavelength3d' not in ds['aot'].dims:
            print(f"Skipping {f}: Missing 'aot' or 'wavelength3d'")
            continue
        else:
            print(f'Processing {f}')

        # Apply valid mask at 550 nm
        aod_diag = ds['aot'].sel(wavelength3d=550.)  # 2D array
        chi2 = ds['chi2'].values  # 2D array
        valid_mask = (aod_diag >= 0.05) & (aod_diag <= 5) & (chi2 <= 5) & np.isfinite(aod_diag)

        if not np.any(valid_mask):
            print(f"Skipping {f}: No valid AOT points")
            continue

        # Flattened arrays at valid points
        valid_lats = lat_vals[valid_mask]
        valid_lons = lon_vals[valid_mask]

        df_out = {
            'lat': valid_lats ,#* len(valid_lons),
            'lon': valid_lons , #*len(valid_lats),
            'date': [obs_time] * len(valid_lats)
        }

        # Extract wavelengths and loop over variables
        wvl = ds['wavelength3d'].values
        for var in variables:
            if var not in ds:
                continue

            if var in variables_2d:
                # 2D variable
                var_data = ds[var].values
                df_out[var] = var_data[valid_mask]
            else:
                # 3D variable (wavelength-dependent)
                for iw,w in enumerate(wvl):
                    var_data = ds[var].isel(wavelength3d=iw, drop=True).values  # shape (lat, lon)
                    df_out[f"{var}_{int(w)}nm"] = var_data[valid_mask] 

        # Build DataFrame and write
        df = pd.DataFrame(df_out)
        table = pa.Table.from_pandas(df)

        if pqwriter is None:
            pqwriter = pq.ParquetWriter(parquet_output, table.schema, use_dictionary=True, compression='snappy')

        pqwriter.write_table(table)

    except Exception as e:
        print(f"Failed processing {f} {var}: {e}")

if pqwriter:
    pqwriter.close()
print("SPEXone observation data saved to:", parquet_output)
