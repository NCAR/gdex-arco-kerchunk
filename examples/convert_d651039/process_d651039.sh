#!/usr/bin/bash

# NOTE: This script takes several hours when run serially.   To catch errors, store all output in a log file:
# ./process_d651039.sh 2>&1 | tee run.log

# Where create_kerchunk.py is located
SRC_DIR="/glade/u/home/bonnland/GitRepos/gdex-arco-kerchunk/src"

#OUTPUT_DIR="/glade/u/home/bonnland/scratch/MMLEA_d651039"
OUTPUT_DIR="/glade/u/home/bonnland/scratch/TEMP_DATA"

# Whether to actually create kerchunk references or not
#DRY_RUN=" --dry_run "  # dry_run = TRUE
DRY_RUN=""             # dry_run = FALSE



# Not all directories contain ensemble data, so we process a subset of directories.
PROCESS_DIRS="./dirs_ensemble.txt"
#PROCESS_DIRS="./dirs_test.txt"
for dir in `cat $PROCESS_DIRS`; do
    MODEL=`echo ${dir} | cut -d'/' -f 1`
    VAR_TYPE=`echo ${dir} | cut -d'/' -f 2`
    VAR=`echo ${dir} | cut -d'/' -f 3`

    # Replace underscores with dashes in model name
    MODEL="${MODEL//_/-}"
    OUTFILE_PREFIX="${VAR}_${VAR_TYPE}_${MODEL}"
    
    INPUT_DIR="/glade/campaign/collections/gdex/data/d651039/${dir}"

    # Options used on every run
    #CLUSTER="--cluster pbs"
    #CLUSTER="--cluster local"
    CLUSTER="--cluster single"
    STANDARD_OPTIONS="$DRY_RUN --action combine --concat_new_dim ensemble --output_format json $CLUSTER --directory $INPUT_DIR --output_directory $OUTPUT_DIR"


    # Include/exclude patterns match specific scenarios (historical, ssp585, historical_ssp585, etc.)
    output_file="${OUTFILE_PREFIX}_historical.json"
    if [ ! -f "${OUTPUT_DIR}/${output_file}" ]; then
        python $SRC_DIR/create_kerchunk.py  --regex ".*(historical).*"  --regex_exclude ".*(cmip6|smbb|rcp|ssp).*" --filename ${output_file} ${STANDARD_OPTIONS}
    else
        echo; echo "${OUTPUT_DIR}/${output_file} exists, skipping."; echo
    fi
    
    output_file="${OUTFILE_PREFIX}_ssp370.json"
    if [ ! -f "${OUTPUT_DIR}/${output_file}" ]; then
        python $SRC_DIR/create_kerchunk.py  --regex ".*(ssp370).*"  --regex_exclude ".*(historical).*"  --filename ${output_file} ${STANDARD_OPTIONS}
    else
        echo; echo "${OUTPUT_DIR}/${output_file} exists, skipping."; echo
    fi
    
    # Ignore files in folders named "temp"; found in /glade/campaign/collections/gdex/data/d651039/miroc6_lens/Omon/zos/temp
    TEMP_EXCLUDE=".*(/temp/).*"
    output_file="${OUTFILE_PREFIX}_ssp585.json"
    if [ ! -f "${OUTPUT_DIR}/${output_file}" ]; then
        python $SRC_DIR/create_kerchunk.py  --regex ".*(ssp585).*"  --regex_exclude "($TEMP_EXCLUDE|.*(historical).*)"  --filename ${output_file} ${STANDARD_OPTIONS}
    else
        echo; echo "${OUTPUT_DIR}/${output_file} exists, skipping."; echo
    fi
    
    output_file="${OUTFILE_PREFIX}_historical_rcp85.json"
    if [ ! -f "${OUTPUT_DIR}/${output_file}" ]; then
        python $SRC_DIR/create_kerchunk.py  --regex ".*(historical_rcp85).*"  --filename ${output_file} ${STANDARD_OPTIONS}
    else
        echo; echo "${OUTPUT_DIR}/${output_file} exists, skipping."; echo
    fi
    
    output_file="${OUTFILE_PREFIX}_historical_ssp370.json"
    if [ ! -f "${OUTPUT_DIR}/${output_file}" ]; then
        python $SRC_DIR/create_kerchunk.py  --regex ".*(historical_ssp370).*" --regex_exclude ".*(cmip6|smbb).*" --filename ${output_file} ${STANDARD_OPTIONS}
    else
        echo; echo "${OUTPUT_DIR}/${output_file} exists, skipping."; echo
    fi
    
    output_file="${OUTFILE_PREFIX}_historical_ssp585.json"
    python $SRC_DIR/create_kerchunk.py  --regex ".*(historical_ssp585).*" --filename ${output_file} ${STANDARD_OPTIONS}

    # Special cases for CESM2 LENS data
    if [[ "$dir" == *"cesm2_lens/"* ]]; then
        echo; echo; echo "Processing special cases for CESM2_LENS"; echo; echo
         
        output_file="${OUTFILE_PREFIX}_cmip6_historical_ssp370.json"
        if [ ! -f "${OUTPUT_DIR}/${output_file}" ]; then
            python $SRC_DIR/create_kerchunk.py  --regex ".*(cmip6_historical_ssp370).*" --filename ${output_file} ${STANDARD_OPTIONS}
        else
            echo; echo "${OUTPUT_DIR}/${output_file} exists, skipping."; echo
        fi
        
        output_file="${OUTFILE_PREFIX}_smbb_historical_ssp370.json"
        if [ ! -f "${OUTPUT_DIR}/${output_file}" ]; then
            python $SRC_DIR/create_kerchunk.py  --regex ".*(smbb_historical_ssp370).*" --filename ${output_file} ${STANDARD_OPTIONS}
        else
            echo; echo "${OUTPUT_DIR}/${output_file} exists, skipping."; echo
        fi
    fi
done

