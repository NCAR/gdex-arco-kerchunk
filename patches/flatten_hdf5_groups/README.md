
## Flattening Datafiles with HDF5 Groups

The IMERG monthly and half-hourly datasets (**d736000** and **d731000**) consist of files with HDF5 groups.   The monthly files contain all coordinate and data variables within the group "Grid".   The half-hourly files contain all variables within the group "Grid" and nested group "Grid/Intermediate".   In contrast, the daily files for IMERG (**d734000**) were processed upstream to move all variables to the global level.

This directory contains a special version of ``create_kerchunk.py`` that flattens all HDF5 group data and places all variables in the global namespace.  Because this is such an unusual case and a healthy amount of code was added to handle HDF5 groups, it was decided that a special, standalone version of ``create_kerchunk.py`` makes more sense than creating extra code bloat in the original version. 

**NOTE**:  This version uses an extra option ``--h5_coord_group <group name>`` to specify which group contains the coordinate dimensions for concatenation using kerchunk.  For IMERG data files, use ``--h5_coord_group Grid``.