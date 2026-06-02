#!/usr/bin/bash

# Where create_kerchunk.py is located
SRC_DIR="/glade/u/home/bonnland/GitRepos/gdex-arco-kerchunk/src"

OUTPUT_DIR="/glade/u/home/bonnland/scratch/MMLEA_d651039"

# Whether to actually create kerchunk references or not
#DRY_RUN=" --dry_run "  # dry_run = TRUE
DRY_RUN=""             # dry_run = FALSE

# Not all directories contain ensemble data, so we process a subset of directories.
#PROCESS_DIRS="./dirs_ensemble.txt"
PROCESS_DIRS="./dirs_test.txt"
for dir in `cat $PROCESS_DIRS`; do
    MODEL=`echo ${dir} | cut -d'/' -f 1`
    VAR_TYPE=`echo ${dir} | cut -d'/' -f 2`
    VAR=`echo ${dir} | cut -d'/' -f 3`

    # Replace underscores with dashes in model name
    MODEL="${MODEL//_/-}"
    OUTFILE_PREFIX="${MODEL}_${VAR_TYPE}_${VAR}"
    
    INPUT_DIR="/glade/campaign/collections/gdex/data/d651039/${dir}"

    # Options used on every run
    #CLUSTER="--cluster pbs"
    #CLUSTER="--cluster local"
    CLUSTER="--cluster single"
    STANDARD_OPTIONS="$DRY_RUN --action combine --concat_ensemble --output_format json $CLUSTER --directory $INPUT_DIR --output_directory $OUTPUT_DIR"

    output_file="${OUTFILE_PREFIX}_historical.json"
    python $SRC_DIR/create_kerchunk.py  --regex ".*(historical).*"  --regex_exclude ".*(rcp|ssp).*" --filename ${output_file} ${STANDARD_OPTIONS}
    
    output_file="${OUTFILE_PREFIX}_ssp370.json"
    python $SRC_DIR/create_kerchunk.py  --regex ".*(ssp370).*"  --regex_exclude ".*(historical).*"  --filename ${output_file} ${STANDARD_OPTIONS}

    output_file="${OUTFILE_PREFIX}_ssp585.json"
    python $SRC_DIR/create_kerchunk.py  --regex ".*(ssp585).*"  --regex_exclude ".*(historical).*"  --filename ${output_file} ${STANDARD_OPTIONS}

    output_file="${OUTFILE_PREFIX}_historical_rcp85.json"
    python $SRC_DIR/create_kerchunk.py  --regex ".*(historical_rcp85).*"  --filename ${output_file} ${STANDARD_OPTIONS}

    output_file="${OUTFILE_PREFIX}_historical_ssp370.json"
    python $SRC_DIR/create_kerchunk.py  --regex ".*(historical_ssp370).*" --filename ${output_file} ${STANDARD_OPTIONS}

    output_file="${OUTFILE_PREFIX}_historical_ssp585.json"
    python $SRC_DIR/create_kerchunk.py  --regex ".*(historical_ssp585).*" --filename ${output_file} ${STANDARD_OPTIONS}

done
