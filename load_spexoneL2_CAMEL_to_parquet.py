for f in spex_files[-10:]:
    with xr.open_dataset(f , group='geophysical_data', decode_timedelta=True) as geophys_ds, \
            xr.open_dataset(f, group='geolocation_data', decode_timedelta=True) as geoloc_ds, \
            xr.open_dataset(f, group='diagnostic_data', decode_timedelta=True) as diag_ds:
            # Check for required variables
        if 'aot' not in geophys_ds or 'latitude' not in geoloc_ds or 'longitude' not in geoloc_ds or 'chi2' not in diag_ds:
            print(f"--> Skipping: Missing required variables")
            continue

        # Check for wavelength3d dimension
        if 'wavelength3d' not in geophys_ds['aot'].dims:
            print(f"--> Skipping: No wavelength3d dimension in aot")
            continue
        print(f'open {f}')
        # Extract data for AOT at 550 nm (wavelength3d=7)
        aot = geophys_ds['aod_{}'].isel(wavelength3d=7).values
        lat = geoloc_ds['latitude'].values
        lon = geoloc_ds['longitude'].values
        chi2 = diag_ds['chi2'].values
        
        # Ensure arrays have compatible shapes
        min_rows = min(aot.shape[0], chi2.shape[0], lat.shape[0], lon.shape[0])
        if min_rows == 0:
            print(f"--> Skipping: No valid data points")
            # continue

        # Trim arrays to the same size
        aot = aot[:min_rows, :]
        chi2 = chi2[:min_rows, :]
        lat = lat[:min_rows, :]
        lon = lon[:min_rows, :]

        # Filter data: AOT between 0 and 5, chi2 <= 5, and valid numbers
        mask = (aot >= 0) & (aot <= 5) & (chi2 <= 5) & np.isfinite(aot)
        aot_valid = aot[mask]
        lat_valid = lat[mask]
        lon_valid = lon[mask]
    aod = ds['aot']
    date = ds.attrs.get("time_coverage_start")
    print(f'extracting {date}')
    # Compute valid mask at 550 nm
    valid_mask = aod.sel(wavelength=550.) >= 0.05
    
    lat_vals = ds['lat'].values  # 1D
    lon_vals = ds['lon'].values  # 1D

    wvl = wavelength
    valid_lat_idx, valid_lon_idx = np.where(valid_mask.values)
    if len(valid_lat_idx) == 0:
        continue  # Skip file if no valid points
    
    # Get lat/lon for valid points
    valid_lats = lat_vals[valid_lat_idx]
    valid_lons = lon_vals[valid_lon_idx]
    
    df_out = {
        'lat': valid_lats,
        'lon': valid_lons,
        'date': [date] * len(valid_lats)
    }
    
    # Loop through selected variables and wavelengths
    for var in variables:
        if var in variables_2d:
            # Check if 'wavelength' is a dimension
            if 'wavelength' in ds[var].dims:
                var_data = ds[var].isel(wavelength=0, drop=True).values  # shape: (lat, lon)
            else:
                var_data = ds[var].values  # shape: (lat, lon)
            
            valid_values = var_data[valid_lat_idx, valid_lon_idx]
            df_out[f"{var}"] = valid_values
        else:
            for w in wvl:
                var_data = ds[var].sel(wavelength=w, drop=True).values  # shape: (lat, lon)
                valid_values = var_data[valid_lat_idx, valid_lon_idx]
                df_out[f"{var}_{w}nm"] = valid_values

    # Create DataFrame and write to Parquet
    df = pd.DataFrame(df_out)
    table = pa.Table.from_pandas(df)
    
    if pqwriter is None:
        pqwriter = pq.ParquetWriter(parquet_path, table.schema, use_dictionary=True, compression='snappy')
    
    pqwriter.write_table(table)

# Close the writer at the end
if pqwriter:
    pqwriter.close()
