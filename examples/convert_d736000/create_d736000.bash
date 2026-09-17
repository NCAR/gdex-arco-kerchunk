#!/bin/bash 

#INPUT_DIRECTORY="/glade/campaign/collections/rda/data/d736000/gpm_3imergm_v07"
INPUT_DIRECTORY="/gdex/data/d736000/gpm_3imergm_v07"
OUTPUT_DIRECTORY="."

# Note the use of a "patched", special-purpose version of create_kerchunk.py to flatten HDF5 data with Groups.
# The group "Grid" contains all coordinate and data variables.
SRC_DIRECTORY="/glade/u/home/bonnland/GitRepos/gdex-arco-kerchunk/patches/flatten_hdf5_groups"

outfile="GPM_3IMERGM_07.json"

$SRC_DIRECTORY/create_kerchunk.py --action combine --make_remote --h5_coord_group Grid -f $outfile --directory $INPUT_DIRECTORY --output_directory $OUTPUT_DIRECTORY --output_format json