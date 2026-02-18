#!/usr/bin/env python
"""
Creates kerchunk sidecar files or combined kerchunk files for a directory structure.

The script can be used to either create individual kerchunk sidecar files for each data file in a directory structure,
or to create a combined kerchunk reference file that aggregates multiple data files.

Parquet reference files is supported for combine action. 

Usage:
    python create_kerchunk.py --action <combine|sidecar> --directory <data directory> [options]

Test command:
    python create_kerchunk.py --action sidecar --directory /path/to/data --output_directory /path/to/output
    python create_kerchunk.py --action combine --directory /gdex/data/d640000/bnd_ocean/194907 --output_directory /glade/u/home/chiaweih/Kerchunk_experiments/test_json --extensions nc --filename combined_kerchunk.json --dry_run
    python create_kerchunk.py --action combine --directory /gdex/data/d640000/bnd_ocean/194907 --output_directory /glade/u/home/chiaweih/Kerchunk_experiments/test_json --extensions nc --filename combined_kerchunk.json 
    python create_kerchunk.py --action combine --directory /glade/campaign/collections/gdex/data/d640000/anl_surf125/194907 --output_directory /glade/u/home/chiaweih/Kerchunk_experiments/test_json --extensions nc --filename anl_surf125.194907.parq --output_format parquet --make_remote
    python create_kerchunk.py --action combine --directory /glade/campaign/collections/gdex/data/d640000/anl_surf125/194907 --output_directory /lustre/desc1/scratch/chiaweih/d640000.jra3q/kerchunk_osdf_fix/osdf_protocol --extensions nc --filename anl_surf125.194907.json --output_format json --make_remote
    --action combine --directory /glade/campaign/collections/gdex/data/d651030/BHIST/b.e13.BHISTC5.ne30_g16.cesm-ihesp-hires1.0.42-1920-2005.009/ocn/proc/tseries/month_1 --output_directory /lustre/desc1/scratch/chiaweih/d651030.mesaclip_lowres/kerchunk --extensions nc --filename b.e13.BHISTC5.ne30_g16.cesm-ihesp-hires1.0.42-1920-2005.009.ocn.month_1.parq --output_format parquet --cluster PBS --make_remote  --chunk_overrides moc_components:3,256,1 transport_components:5,256,1  transport_regions:2,256,1
"""

# import codecs
# import pdb
# from pathlib import Path

import os, sys
import json
import ujson
import argparse
import re
import time
import dask
from dask_jobqueue import PBSCluster
from dask.distributed import LocalCluster
from fsspec.implementations.local import LocalFileSystem
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr

### Global settings

# special keyword to indicate all variables should be separated
ALL_VARIABLES_KEYWORD = "ALL"
PBS_LOCAL_DIR = '/lustre/desc1/scratch/chiaweih/temp_dask'
PBS_LOG_DIR = '/lustre/desc1/scratch/chiaweih/temp_pbs'
# global filesystem object
fs = LocalFileSystem()
# default fs.open(file, **so) arguments dictionary for writing kerchunk reference files
so = dict(mode='rb', anon=True, default_fill_cache=False, default_cache_type='first')
# initial global dask client
_global_client = None


def _get_parser():
    """Creates and returns parser object.
    Returns:
        (argparse.ArgumentParser): Parser object from which to parse arguments.
    """
    description = "Creates kerchunk sidecar files of an entire directory structure."
    prog_name = sys.argv[0] if sys.argv[0] != '' else 'create_kerchunk_sidecar'
    parser = argparse.ArgumentParser(
        prog=prog_name,
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--action', '-a',
        type=str,
        required=True,
        choices=['combine','sidecar'],
        nargs=None,
        metavar='<combine|sidecar>',
        help='Specify whether to create to combine references or create sidecar files.'
    )
    parser.add_argument(
        '--directory', '-d',
        type=str,
        nargs=None,
        metavar='<directory>',
        required=True,
        help="Directory to scan and create kerchunk reference files."
    )
    parser.add_argument(
        '--output_directory', '-o',
        type=str,
        nargs=None,
        metavar='<directory>',
        required=False,
        default='.',
        help="Directory to place output files"
    )
    parser.add_argument(
        '--filename', '-f',
        type=str,
        nargs=None,
        metavar='<output filename>',
        required=False,
        default='',
        help="Filename for output json."
    )
    parser.add_argument(
        '--extensions', '-e',
        type=str,
        required=False,
        nargs='+',
        metavar='<extension>',
        help='Only process files of this extension',
        default=[]
    )
    parser.add_argument(
        '--variables', '-v',
        type=str,
        required=False,
        nargs='+',
        metavar='<variable names>',
        help=(
        "Only gather specific variables.\n"+
        "Variable names are case sensitive.\n"+
        f"Use the special keyword '{ALL_VARIABLES_KEYWORD}' to separate all into individual files."
        ),
        default=[]
    )
    parser.add_argument(
        '--drop_variables', '-dv',
        type=str,
        required=False,
        nargs='+',
        metavar='<variable names>',
        help=(
        "Drop specific variables.\n"+
        "Variable names are case sensitive."
        ),
        default=[]
    )
    parser.add_argument(
        '--cluster', '-c',
        type=str,
        default="local",
        required=False,
        nargs=1,
        metavar='< PBS / single / local >',
        help="""Choose type of dask cluster to use.
        PBS - PBSCluster (defaults to 5 workers)
        single - singleThreaded
        local - localCluster (uses os.ncpus)
        """
    )
    parser.add_argument(
        '--dry_run', '-dr',
        action='store_true',
        required=False,
        help='Do a dry run of processing',
        default=[]
    )
    parser.add_argument(
        '--make_remote', '-mr',
        action='store_true',
        required=False,
        help='Additionally make a remote accessible copy of json',
        default=[]
    )

    def unescaped_str(arg_str):
        """Remove escape characters from argument string."""
        return arg_str.replace("\\\\","\\")

    parser.add_argument(
        '--regex', '-r',
        type=unescaped_str,
        required=False,
        nargs=None,
        metavar='<regular expression>',
        help='Combine references that match'
    )

    parser.add_argument(
        '--output_format', '-of',
        type=str,
        default="json",
        required=False,
        nargs=1,
        metavar='< json / parquet >',
        help='Specify the output format for the combined references.'
    )

    parser.add_argument(
        '--chunk_overrides', '-co',
        type=str,
        required=False,
        nargs='+',
        metavar='<variable:chunks>',
        help=(
            'Override chunk sizes for specific variables in the format: variable_name:dim1,dim2,dim3\n'
            'Example: --chunk_overrides temperature:1,100,200 pressure:1,50,50\n'
            'This modifies the Kerchunk reference without changing the original HDF5 file.'
        ),
        default=[]
    )

    parser.add_argument(
        '--combine_memory', '-cm',
        type=str,
        required=False,
        metavar='<memory>',
        help=(
            'Memory allocation for the combine step (e.g., "128GB", "256GB").\n'
            'After parallel reference generation, restarts Dask with 1 high-memory worker.\n'
            'Only applies when using PBS cluster. Default: uses existing cluster.'
        ),
        default=None
    )

    return parser


def get_cluster(
    cluster_setting,
    num_processes=4,
    local_directory_pbs: str = None,
    log_directory_pbs: str = None
):
    """Starts a cluster given option and create a global client

    Args:
        cluster (str): May be the following
            'PBS' - PBSCluster where 5 workers are used.
            'local' - LocalCluster where number of workers are os.ncpus().
            'single' - single threaded localCluster>
            'k8s' - KubeCluster
        num_processes (int): Number of processes/workers to use. Default 0-use cluster default

    Returns:
        dask.distributed.Client - Client object for all cluster types.
    """
    global _global_client

    cluster_setting = cluster_setting.lower()
    match cluster_setting:
        case "pbs":
            cluster = PBSCluster(
                job_name = 'gdex-kerchunk-hpc',
                cores = 1,
                memory = '10GiB',
                processes = 1,
                account = 'P43713000',
                local_directory = local_directory_pbs,
                log_directory = log_directory_pbs,
                resource_spec = 'select=1:ncpus=1:mem=10GB',
                queue = 'gdex',
                walltime = '24:00:00',
                interface = 'ext'
            )
            cluster.scale(jobs=num_processes)
        case 'single':
            cluster = LocalCluster(n_workers=1, threads_per_worker=1, processes=False)
        case 'k8s':
            try :
                # old version of dask_kubernetes
                from dask_kubernetes import KubeCluster
            except ImportError:
                # new version of dask_kubernetes
                from dask_kubernetes.operator import KubeCluster
            cluster = KubeCluster()
            cluster.scale(jobs=num_processes)
        case 'local':
            cluster = LocalCluster()
        case _: # default use localCluster
            cluster = LocalCluster()

    if _global_client is None:
        _global_client = cluster.get_client()

def cleanup_dask_client():
    """Cleanup global dask client."""
    global _global_client
    if _global_client is not None:
        _global_client.close()
        _global_client = None


def restart_client_for_combine(
    memory='128GB',
    local_directory_pbs: str = None,
    log_directory_pbs: str = None
):
    """Restart Dask client with single high-memory worker for combine operation.
    
    Args:
        memory (str): Memory allocation (e.g., '128GB', '256GB')
        local_directory_pbs (str): Local directory for PBS
        log_directory_pbs (str): Log directory for PBS
    """
    global _global_client
    
    # Close existing client
    if _global_client is not None:
        print(f'Closing existing Dask client')
        _global_client.close()
        _global_client = None
    
    # Create new high-memory cluster
    print(f'Creating new Dask client with 1 worker, {memory} memory for combine operation')
    cluster = PBSCluster(
        job_name = 'gdex-kerchunk-combine',
        cores = 1,
        memory = memory,
        processes = 1,
        account = 'P43713000',
        local_directory = local_directory_pbs,
        log_directory = log_directory_pbs,
        resource_spec = f'select=1:ncpus=1:mem={memory}',
        queue = 'gdex',
        walltime = '24:00:00',
        interface = 'ext'
    )
    cluster.scale(jobs=1)  # Only 1 worker needed
    _global_client = cluster.get_client()
    print(f'New client ready')


def override_chunks(reference_struct, chunk_overrides):
    """Override chunk information in a Kerchunk reference structure.
    
    This modifies the .zarray metadata for specified variables to use different
    chunk sizes without modifying the original HDF5 file.
    
    Parameters
    ----------
    reference_struct : dict
        The Kerchunk reference structure (from h5chunks.translate())
    chunk_overrides : dict
        Dictionary mapping variable names to desired chunk tuples
        Example: {'temperature': (1, 100, 200), 'pressure': (1, 50, 50)}
        
    Returns
    -------
    reference_struct : dict
        Modified reference structure with updated chunk information
        
    Examples
    --------
    >>> refs = h5chunks.translate()
    >>> refs = override_chunks(refs, {'temp': (1, 256, 256)})
    """

    for var_name, new_chunks in chunk_overrides.items():
        # Look for the .zarray metadata entry for this variable
        zarray_key = f"{var_name}/.zarray"
        if zarray_key in reference_struct['refs']:
            # Parse the .zarray JSON string
            zarray_data = json.loads(reference_struct['refs'][zarray_key])

            # Update the chunks field
            old_chunks = zarray_data.get('chunks', 'unknown')
            zarray_data['chunks'] = list(new_chunks)

            # Write back the modified .zarray
            reference_struct['refs'][zarray_key] = json.dumps(zarray_data)

            print(f"Updated chunks for '{var_name}': {old_chunks} -> {new_chunks}")
        else:
            print(f"Warning: Variable '{var_name}' not found in reference structure")

    return reference_struct


def parse_chunk_overrides(chunk_override_args):
    """Parse chunk override arguments from command line.
    
    Parameters
    ----------
    chunk_override_args : list of str
        List of strings in format 'variable:dim1,dim2,dim3'
        Example: ['temperature:1,100,200', 'pressure:1,50,50']
        
    Returns
    -------
    chunk_dict : dict
        Dictionary mapping variable names to chunk tuples
        Example: {'temperature': (1, 100, 200), 'pressure': (1, 50, 50)}
    """
    chunk_dict = {}
    
    for arg in chunk_override_args:
        try:
            # print(f"Parsing chunk override argument: {arg}")
            var_name, chunks_str = arg.split(':', 1)
            # print(f"Argument details: variable={var_name}, chunks={chunks_str}")
            chunks = tuple(int(x.strip()) for x in chunks_str.split(','))
            chunk_dict[var_name.strip()] = chunks
        except ValueError as e:
            print(f"Warning: Could not parse chunk override '{arg}': {e}")
            print(f"Expected format: variable_name:dim1,dim2,dim3")
    
    return chunk_dict


def gen_reference(file_url, output_format='json', write_reference=False, chunk_overrides=None):
    """Generate kerchunk json structure for a single file.
    
    Parameters
    -----------
    file_url : str
        path to file to generate kerchunk json structure for
    output_format : str
        output format for the generated kerchunk structure.
        Default is 'json'. If 'parquet' is specified, the output will be in parquet format.
    write_reference : bool
        whether to write the json structure to a sidecar file.
        Default is False and returns the json structure without writing to file.
        This is useful for dask delayed processing of multiple files combined later.
    chunk_overrides : dict, optional
        Dictionary mapping variable names to desired chunk tuples to override
        incorrect chunk information in the original file.
        Example: {'temperature': (1, 100, 200), 'pressure': (1, 50, 50)}

    Returns
    --------
    reference_struct : dict
        kerchunk json structure

    Notes
    -----
    - sidecar file will be named <file_url>.json
    - the function also checks if the sidecar file already exists
    - the function will write the sidecar file if it does not exist and
      write_json is True

    """
    file_basename = os.path.basename(file_url)

    if output_format.lower() == 'parquet':
        outfile = f'{file_basename}.parq'
    elif output_format.lower() == 'json':
        outfile = f'{file_basename}.json'
    else:
        print(f'output format "{output_format}" not recognized. Supported formats are "json" and "parquet".')
        sys.exit(1)

    print(f'generating {file_basename} references')

    # skip build and just load and return existing json structure
    # TODO: check read construct validity
    if os.path.exists(outfile):
        print(f'{outfile} exists, skipping')
        with fs.open(outfile, 'rb') as f:
            reference_struct = ujson.loads(f.read().decode())
        return reference_struct

    # build json structure if not exists
    with fs.open(file_url, **so) as infile:
        # set vlen_encode to 'leave' to avoid issues with string variable (d640000)
        # set error to 'ignore' to skip over string decoding issues
        # check https://fsspec.github.io/kerchunk/reference.html#kerchunk.hdf.SingleHdf5ToZarr
        # for more details
        h5chunks = kerchunk.hdf.SingleHdf5ToZarr(
            infile,
            file_url,
            inline_threshold=366,
            vlen_encode='leave',
            error='ignore'
        )
        # year = file_url.split('/')[-1].split('.')[0]
        reference_struct = h5chunks.translate()
        
        # Override chunk information if provided
        if chunk_overrides:
            reference_struct = override_chunks(reference_struct, chunk_overrides)
        
        if write_reference and output_format.lower() == 'json':
            with fs.open(outfile, 'wb') as f:
                print(f'writing {outfile}')
                f.write(ujson.dumps(reference_struct).encode())
        elif write_reference and output_format.lower() == 'parquet':
            from kerchunk import df
            print(f'writing {outfile}')
            df.refs_to_dataframe(reference_struct, outfile)

    return reference_struct

def create_directories(dirs, base_path='./'):
    """Create directories in dirs list if they do not exist."""
    for i in dirs:
        directory_path = os.path.join(base_path, i)
        # create directory if it does not exist
        if not os.path.exists(directory_path):
            os.mkdir(directory_path)
    return None

def matches_extension(filename, extensions):
    """Check if filename matches one of the extensions."""
    if len(extensions) == 0:
        return True
    for j in extensions:
        if re.match(f'.*{j}$', filename):
            return True
    return False

def process_kerchunk_sidecar(directory, output_directory='.', output_format='json', extensions=None, dry_run=False, chunk_overrides=None):
    """Traverse files in `directory` and create kerchunk sidecar files."""

    if extensions is None:
        extensions = []

    try:
        os.stat(directory)
    except FileNotFoundError:
        print(f'Directory "{directory}" cannot be found')
        sys.exit(1)

    # change working directory to output directory
    try:
        os.stat(output_directory)
    except FileNotFoundError:
        print(f'Output directory "{output_directory}" cannot be found')
        sys.exit(1)
    os.chdir(output_directory)

    for cur_dir, child_dirs, files in os.walk(directory):
        # data file location information
        cur_dir_base = os.path.basename(os.path.normpath(cur_dir))

        # check if reference file location has a subdirectory with the same name as
        # the cur_dir_base at data file location
        try:
            os.stat(cur_dir_base)
            os.chdir(cur_dir_base)
        except FileNotFoundError:
            pass

        # run reference generation for each file (if file matches extension and exists)
        for f in files:
            if matches_extension(f, extensions):
                # print file being processed
                print(f)
                # create json reference sidecar file
                if not dry_run:
                    gen_reference(os.path.join(cur_dir,f), output_format=output_format, write_reference=True, chunk_overrides=chunk_overrides)

        if len(child_dirs) == 0:
            # if get to the end of the branch, go back up one level
            os.chdir('..')
        else:
            # create child directories
            create_directories(child_dirs)

def find_files(directory, regex, extensions):
    """Traverse in the directory to find files."""
    all_files = []
    # Handle empty regex - match all files if regex is empty
    if regex:
        pattern = re.compile(regex)
    else:
        pattern = re.compile(".*")  # Match everything

    for cur_dir, _, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(cur_dir, file)
            if matches_extension(full_path, extensions) and pattern.match(full_path):
                full_path = os.path.normpath(full_path)
                all_files.append(full_path)
    return all_files

def get_time_variable(filename):
    """Get time variable name in the file
    Will try different methods for finding lat in decreasing authority.

    Parameters
    ----------
    filename : str
        path to file to examine
    
    Returns
    -------
    time_varname : str or None
        name of time variable if found, else None (need to check manually)
    """
    import xarray
    ds = xarray.open_dataset(filename)

    for key, value in ds.coords.items():
        if 'standard_name' in value.attrs and value.attrs['standard_name'] == 'time':
            return key
        if 'standard_name' in value.attrs and value.attrs['standard_name'] == 'forecast_reference_time':
            return key
        if 'long_name' in value.attrs and value.attrs['long_name'] == 'time':
            return key
        if 'short_name' in value.attrs and value.attrs['short_name'] == 'time':
            return key
        if key.lower() == 'time':
            return key
        if 'units' in value.attrs and 'minutes since' in value.attrs['units']:
            return key

    return None

def separate_vars(refs, var_names):
    """Extracts specific variables from reference files object."""
    # preset keep variables
    keep_values = set([
        '.zgroup',
        '.zattrs',
        'Time',
        'XLAT',
        'XLONG',
        'XLAT_U',
        'XLONG_U',
        'XLAT_V',
        'XLONG_V',
        'XTIME',
    ])
    # update keep variables with user specified variables in var_names
    keep_values.update(var_names)

    # process each reference file object
    # only keep variables in keep_values
    updated_refs = []
    for ref in refs:
        new_json = {}
        for i in ref['refs']:
            varname = i.split('/')[0]
            if varname in keep_values:
                new_json[i] = ref['refs'][i]
        ref['refs'] = new_json
        updated_refs.append(ref)
    return updated_refs

def drop_vars(refs, var_names):
    """Drops specific variables from reference files object."""

    # update keep variables with user specified variables in var_names
    drop_values = set(var_names)

    # process each reference file object
    # only keep variables not in drop_values
    updated_refs = []
    for ref in refs:
        new_json = {}
        for i in ref['refs']:
            varname = i.split('/')[0]
            if varname not in drop_values:
                new_json[i] = ref['refs'][i]
        ref['refs'] = new_json
        updated_refs.append(ref)
    return updated_refs

# def separate_combine_write_all_vars(refs, var_names, make_remote=False):
#     """Extracts specific variables from refs.
#     """
#     updated_refs = separate_vars(refs, var_names)

#     print('combining')
#     mzz = MultiZarrToZarr(
#            updated_refs,
#            concat_dims=["time"],
#            #coo_map='QSNOW',
#         )
#     multi_kerchunk = mzz.translate()
#     write_kerchunk(output_directory, multi_kerchunk, regex, variables, output_filename, make_remote)

def write_combined_kerchunk(output_directory, multi_kerchunk, regex=None, output_filename="",output_format="json", make_remote=False):
    """Write kerchunk .json for combined kerchunk.

    Args:
        output_directory (str): Directory to  place json files.
        multi_kerchunk (list): list of Kerchunk dicts.
        regex (str): regex used to search over source files (used to guess output filename).
        output_filename (str): name of the output file.
        make_remote (bool): whether to make the output file remote.
    """
    # determine file extension based on output format
    if output_format.lower() == 'json':
        file_extension = '.json'
    elif output_format.lower() == 'parquet':
        file_extension = '.parq'
    else:
        print(f'output format "{output_format}" not recognized. Supported formats are "json" and "parquet".')
        sys.exit(1)

    # define output filename
    if output_filename:
        # check if output filename ends with the correct file extension
        if output_filename.strip()[-len(file_extension):] != file_extension:
            output_filename = output_filename + file_extension
        output_fname = os.path.join(output_directory, output_filename)
    elif regex:
        guessed_filename = regex.replace('*','').replace('.','').replace('$','').replace('^','').replace('[','').replace(']','')
        output_fname = os.path.join(output_directory, guessed_filename)
    else:
        output_fname =  os.path.join(output_directory, f'combined_kerchunk{file_extension}')

    # write output file
    if output_format.lower() == 'json':
        with open(f"{output_fname}", "wb") as f:
            f.write(ujson.dumps(multi_kerchunk).encode())
        
        print(f'Created: {output_fname}')

        if make_remote:
            import convert_ref_file_loc
            convert_ref_file_loc.main(output_fname, output_fname.replace(file_extension,f'-remote{file_extension}'))

    elif output_format.lower() == 'parquet':
        from kerchunk import df
        df.refs_to_dataframe(multi_kerchunk, output_fname)

        print(f'Created: {output_fname}')

        if make_remote:
            import convert_ref_file_loc
            convert_ref_file_loc.main_parquet(multi_kerchunk, output_fname.replace(file_extension,f'-remote{file_extension}'))


def process_kerchunk_combine(
    directory,
    output_directory='.',
    extensions=None,
    regex=None,
    dry_run=False,
    variables=None,
    drop_variables=None,
    output_filename="",
    make_remote=False,
    output_format="json",
    chunk_overrides=None,
    combine_memory=None,
    cluster_type='local'
):
    """Traverse files in `directory` and create kerchunk aggregated files.
    
    Parameters
    ----------
    combine_memory : str, optional
        If specified and cluster_type is 'pbs', restarts Dask client with
        1 high-memory worker after reference generation completes.
        Example: '128GB', '256GB'
    cluster_type : str
        Type of cluster being used ('pbs', 'local', etc.)
    """

    # set default arguments
    if extensions is None:
        extensions = []
    if variables is None:
        variables = []

    # check if data directory exists
    try:
        os.stat(directory)
    except FileNotFoundError:
        print(f'Data directory "{directory}" cannot be found')
        sys.exit(1)

    # find files to process
    files = find_files(directory, regex, extensions)
    files = sorted(files)
    print(f'Number of files: {len(files)}')

    time_varname = get_time_variable(files[0])
    # check if time variable name is found
    if time_varname is None:
        print('Could not determine time variable name')
        sys.exit(1)

    # dask delayed processing
    lazy_results = []
    if dry_run:
        print(f'processing {files}')
        print(f'{len(files)} files to process')
        sys.exit(1)

    # monitor memory usage on the client process
    import psutil
    process = psutil.Process(os.getpid())

    all_refs = []
    for f in files:
        lazy_result = dask.delayed(gen_reference)(f, output_format=output_format, write_reference=False, chunk_overrides=chunk_overrides)
        lazy_results.append(lazy_result)
        # Split up large jobs
        if len(lazy_results) > 5000:
            start = time.time()
            print(f'Intermediate processing ({len(all_refs)}/{len(files)})')
            tmp_refs = dask.compute(*lazy_results)
            all_refs.extend(tmp_refs)
            lazy_results = []
            end = time.time()
            print(f'Done intermediate. Elapsed time ({end-start} seconds)')

            # print memory used on client process
            memory_gb = process.memory_info().rss / 1e9
            print(f'Client process memory usage (GB): {memory_gb:.2f}')
                
            # Get actual memory usage from Dask cluster
            if _global_client is not None:
                worker_info = _global_client.scheduler_info()['workers']
                total_memory_used = sum(w['metrics']['memory'] for w in worker_info.values())
                print(f'Total memory used by Dask workers (GB): {total_memory_used / 1e9:.2f}')
                print('Total memory used for references (GB): ', sum([sys.getsizeof(r)/1e9 for r in all_refs]))

    all_refs.extend(dask.compute(*lazy_results))

    # print total number of references generated
    print(f'Reference generation complete. Total references: {len(all_refs)}')
    
    # After compute(), data is in client process memory (not on workers anymore)
    memory_gb = process.memory_info().rss / 1e9
    print(f'Client process memory usage (GB): {memory_gb:.2f}')
    
    # Restart client with high memory for combine operation if specified
    # if combine_memory and cluster_type.lower() == 'pbs':
    #     restart_client_for_combine(
    #         memory=combine_memory,
    #         local_directory_pbs=PBS_LOCAL_DIR,
    #         log_directory_pbs=PBS_LOG_DIR
    #     )

    # if len(variables) == 1 and variables[0] == ALL_VARIABLES_KEYWORD:
    #     separate_combine_write_all_vars(all_refs, variables, make_remote)
    #     sys.exit(1)
    if len(variables) > 0:
        all_refs = separate_vars(all_refs, variables)

    if len(drop_variables) > 0:
        all_refs = drop_vars(all_refs, drop_variables)

    print('combining')
    
    mzz = MultiZarrToZarr(
           all_refs,
           concat_dims=[time_varname],
           #coo_map='QSNOW',
        )
    print('Total memory used for combined references (GB): ', sys.getsizeof(mzz)/1e9)

    print('create aggregation')
    # Submit the translate operation to the worker
    # future = _global_client.submit(lambda: mzz.translate())
    # multi_kerchunk = future.result()

    multi_kerchunk = mzz.translate()

    print('write out')
    write_combined_kerchunk(
        output_directory,
        multi_kerchunk,
        regex,
        output_filename,
        output_format,
        make_remote
    )


def main():
    """Entrypoint for command line application."""
    parser = _get_parser()
    print(sys.argv)
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()
    print(args)

    # Parse chunk overrides if provided
    chunk_overrides = None
    if args.chunk_overrides:
        chunk_overrides = parse_chunk_overrides(args.chunk_overrides)
        if chunk_overrides:
            print(f"Chunk overrides: {chunk_overrides}")

    # initialize dask client
    get_cluster(
        cluster_setting = args.cluster[0],
        num_processes=10,
        local_directory_pbs=PBS_LOCAL_DIR,
        log_directory_pbs=PBS_LOG_DIR
    )

    if args.action == 'sidecar':
        process_kerchunk_sidecar(
            args.directory,
            args.output_directory,
            extensions=args.extensions,
            dry_run=args.dry_run,
            chunk_overrides=chunk_overrides
        )
    elif args.action == 'combine':
        process_kerchunk_combine(
            args.directory,
            args.output_directory,
            extensions=args.extensions,
            dry_run=args.dry_run,
            variables=args.variables,
            drop_variables=args.drop_variables,
            regex=args.regex,
            output_filename=args.filename,
            make_remote=args.make_remote,
            output_format=args.output_format[0],
            chunk_overrides=chunk_overrides,
            combine_memory=args.combine_memory,
            cluster_type=args.cluster[0]
        )
    else:
        print(f'action type "{args.action}" not recognized')
        sys.exit(1)

    # cleanup dask client
    cleanup_dask_client()

if __name__ == '__main__':
    main()
