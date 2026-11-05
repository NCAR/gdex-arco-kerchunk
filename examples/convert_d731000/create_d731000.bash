#!/bin/bash 

# For testing purposes, use a small subset of the data.
#INPUT_DIRECTORY="/glade/campaign/collections/gdex/data/d731000/gpm_3imerghh_v07/2025/04/05"
#OUTPUT_DIRECTORY="."

INPUT_DIRECTORY="/glade/campaign/collections/gdex/data/d731000/gpm_3imerghh_v07"
OUTPUT_DIRECTORY="/glade/derecho/scratch/bonnland/IMERG_d731000_NEW"

# Note the use of a "patched", special-purpose version of create_kerchunk.py to flatten HDF5 data with Groups.
# The group "Grid" contains all coordinate and data variables.
SRC_DIRECTORY="/glade/u/home/bonnland/GitRepos/gdex-arco-kerchunk/patches/flatten_hdf5_groups"


#outfile="GPM_3IMERGDF_v07.json"
# $SRC_DIRECTORY/create_kerchunk.py --action combine --h5_coord_group Grid --variables ALL --make_remote -f $outfile --directory $INPUT_DIRECTORY --output_directory $OUTPUT_DIRECTORY --output_format json

outfile="GPM_3IMERGHH_v07"
$SRC_DIRECTORY/create_kerchunk.py --action combine --h5_coord_group Grid --variables ALL --make_remote --cluster single -f $outfile --directory $INPUT_DIRECTORY --output_directory $OUTPUT_DIRECTORY --output_format parquet