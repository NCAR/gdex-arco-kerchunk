#!/usr/bin/env python
import sys
import copy
from kerchunk import df
# import os
# import re
# import pdb


def main(filename, outfile):
    """ Convert local file path to remote https file path
    There are two remote paths:
    1. https://data.gdex.ucar.edu
    2. https://osdf-director.osg-htc.org/ncar/gdex

    The remote referece files will be saved as:
    1. [outfile]-https.json
    2. [outfile]-osdf.json
    respectively.

    The 

    Parameters
    ----------
    filename : str
        Input local reference file name
    outfile : str
        Output remote reference file name. 

    """
    out_filename = outfile.split('.json')[0]+'-https.json'
    ofile = open(out_filename, 'w', encoding='utf-8')
    osdf_filename = outfile.split('.json')[0]+'-osdf.json'
    osdf_ofile = open(osdf_filename, 'w', encoding='utf-8')
    with open(filename, 'r', encoding='utf-8') as fh:
        for l in fh:
            # Find local file path pattern and replace with remote https path
            # Note: change to new GDEX path
            match = '\\/glade\\/campaign\\/collections\\/gdex\\/data'
            replacement = 'https:\\/\\/data.gdex.ucar.edu'
            # replacement_osdf = 'https:\\/\\/osdf-director.osg-htc.org\\/ncar\\/gdex'
            replacement_osdf = 'osdf:\\/\\/\\/ncar\\/gdex'
            # Note : old RDA path
            # match = '\\/gpfs\\/csfs1\\/collections\\/rda\\/data'
            # replacement = 'https:\\/\\/data.rda.ucar.edu'
            # replacement_osdf = 'https:\\/\\/osdf-director.osg-htc.org\\/ncar\\/rda'

            # replace local reference to remote reference  
            str_output =  l.replace(match, replacement)
            str_output_osdf =  l.replace(match, replacement_osdf)

            # output to remote reference file
            ofile.write(str_output)
            osdf_ofile.write(str_output_osdf)
            #print(l)

    print(f'Created: {out_filename}')
    print(f'Created: {osdf_filename}')

def main_parquet(dict_reference, outfile):
    """ Convert local file path to remote https file path in reference dictionary
    There are two remote paths:
    1. https://data.gdex.ucar.edu
    2. osdf:///ncar/gdex

    The remote reference files will be saved as:
    1. [outfile]-https.parquet
    2. [outfile]-osdf.parquet
    respectively.

    Parameters
    ----------
    dict_reference : dict
        Input local parquet reference dictionary
    outfile : str
        Output remote parquet reference file name. 

    """

    # Define path patterns for replacement
    match = '/glade/campaign/collections/gdex/data'
    replacement = 'https://data.gdex.ucar.edu'
    # replacement_osdf = 'https://osdf-director.osg-htc.org/ncar/gdex'
    replacement_osdf = 'osdf:\\/\\/\\/ncar\\/gdex'

    # hard copy the dict_reference to modify
    dict_reference_https = copy.deepcopy(dict_reference)
    dict_reference_osdf = copy.deepcopy(dict_reference)

    # Create a DataFrame from the dictionary
    for chunk_name, chunk_value in dict_reference['refs'].items():
        if isinstance(chunk_value, list):
            # first element is path string
            dict_reference_https['refs'][chunk_name][0] = chunk_value[0].replace(match, replacement)
            dict_reference_osdf['refs'][chunk_name][0] = chunk_value[0].replace(match, replacement_osdf)

    # Generate output filenames
    if outfile.endswith('.parq'):
        base_name = outfile.split('.parq')[0]
    else:
        base_name = outfile
    out_filename_https = base_name + '-https.parq'
    out_filename_osdf = base_name + '-osdf.parq'

    # Write the modified dataframes to parquet files
    df.refs_to_dataframe(dict_reference_https, out_filename_https)
    df.refs_to_dataframe(dict_reference_osdf, out_filename_osdf)

    print(f'Created: {out_filename_https}')
    print(f'Created: {out_filename_osdf}')

if __name__ == "__main__":
    # test if arguments are provided
    if len(sys.argv) < 3:
        print('Convert local file to remote file location')
        print('For JSON files:')
        print(f'  usage: {sys.argv[0]} [filename.json] [outfile.json]')
        sys.exit(1)

    filename = sys.argv[1]
    outfile = sys.argv[2]

    # Default to JSON processing for backwards compatibility
    main(filename, outfile)
