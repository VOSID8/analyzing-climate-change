import xarray as xr
import os

DS = xr.open_dataset("prcp2.nc")
DF = DS.to_dataframe()

DF.to_csv("prcp2pl.csv")


