#!/usr/bin/env python
"""
Change chunk sizes of specified variables in a NetCDF file and save to a new file.
"""

import sys
import xarray


if len(sys.argv) < 3:
    print(f'Usage: ./{sys.argv[0]} [filename] [output filename] [var1] [var2] ...')
    exit(1)


filename = sys.argv[1]
out_filename = sys.argv[2]
variables = sys.argv[3:]

chunk_sizes = {'chunksizes':[1, 17, 339, 456]}
ds = xarray.open_dataset(filename)
encoding = {}
for v in variables:
    encoding[v] = chunk_sizes

ds.to_netcdf(out_filename, encoding=encoding)

