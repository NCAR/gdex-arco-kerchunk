#!/usr/bin/env python
"""
Read a local Kerchunk reference file and print the dataset contents.


"""

import fsspec
import sys
import xarray


def read_local_ref(filename, *args):
    fs = fsspec.filesystem('reference', fo=filename)
    m = fs.get_mapper('')
    ds = xarray.open_dataset(m, engine='zarr', backend_kwargs={'consolidated':False})
    print(ds)

    return ds

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print(f'Usage {sys.argv[0]} [filename]')
        exit(1)
    read_local_ref(sys.argv[1], sys.argv[2:])
