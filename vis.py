import pandas as pd
import matplotlib.pyplot as plt
import cartopy.feature as cfeature

def cartopy_plot(arr,date,bbox):
    '''
    Make a geospatial plot with a cartopy background
    '''
    fig = plt.figure(figsize=(10, 8))
    ax = plt.axes(projection=ccrs.PlateCarree())

    min_lon,min_lat,max_lon,max_lat = bbox
    
    # Set map extent based on data
    ax.set_extent([min_lon - 1, max_lon + 1, min_lat - 1, max_lat + 1], crs=ccrs.PlateCarree())
    
    # Add map features
    ax.stock_img()
    ax.add_feature(cfeature.LAND, facecolor='lightgray', alpha=0.8)
    ax.add_feature(cfeature.OCEAN, alpha=0.8)
    ax.add_feature(cfeature.BORDERS, linestyle=':', linewidth=0.5, alpha=0.8)
    
    mesh = ax.imshow(arr,extent=(min_lon,max_lon,min_lat,max_lat),
                     cmap='hot_r',vmin=0, vmax=1.5, transform=ccrs.PlateCarree())
    
    plt.colorbar(mesh, label='Aerosol Optical Thickness (550 nm)', shrink=0.7)
    date_str = pd.to_datetime(date).strftime("%Y-%m-%d")
    plt.title(f'PACE SPEXone AOT550 ({date_str}) - Composite')
    
    # Save the plot
    # plt.savefig('.png')
    
    plt.show()
    plt.close(fig)