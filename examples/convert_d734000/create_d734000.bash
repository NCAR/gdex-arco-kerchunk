#!/bin/bash 

#INPUT_DIRECTORY="/glade/campaign/collections/rda/data/d734000/gpm_3imergdf_v07"
INPUT_DIRECTORY="/gdex/data/d734000/gpm_3imergdf_v07"
OUTPUT_DIRECTORY="."

# Note that in this IMERG dataset, HDF5 groups have been removed in the input data.
SRC_DIRECTORY="../../src"

outfile="GPM_3IMERGDF_v07.json"

$SRC_DIRECTORY/create_kerchunk.py --action combine --make_remote -f $outfile --directory $INPUT_DIRECTORY --output_directory $OUTPUT_DIRECTORY --output_format json