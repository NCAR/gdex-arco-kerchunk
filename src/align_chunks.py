#!/usr/bin/env python
"""CLI tool to detect and fix NetCDF chunk-size inconsistencies.

This command recursively walks a top-level directory, inspects NetCDF files,
and reports variables whose chunking differs across files in the same folder.
For each mismatch, the script chooses the most common (majority) chunk pattern
as the target.

Behavior:
- Two-stage workflow: 'plan' scans files and writes a parquet rechunk plan;
    'execute' reads the plan and rewrites files with the target chunk encoding.
- In execute stage, rewritten files are written under --output-directory while
    preserving subdirectory structure relative to top_directory.

Usage:
        python src/align_chunks.py plan <top_directory> --output-parquet <plan.parq>
        python src/align_chunks.py execute <top_directory> --input-parquet <plan.parq> --output-directory <out_dir>

Examples:
        # Plan: scan with default pattern
        python src/align_chunks.py plan /gdex/data/d651007 \
                --output-parquet /lustre/desc1/scratch/chiaweih/plan.parq

        # Plan: scan with custom pattern and exclude filter
        python src/align_chunks.py plan /gdex/data/d651007 \
                --pattern "wrf2d_*.nc" --exclude-pattern "*_backup.nc" \
                --output-parquet /lustre/desc1/scratch/chiaweih/plan.parq

        # Execute: rechunk files based on the plan
        python src/align_chunks.py execute /gdex/data/d651007 \
                --input-parquet /lustre/desc1/scratch/chiaweih/plan.parq \
                --output-directory /lustre/desc1/scratch/chiaweih/rechunked
"""


import os
import logging
import argparse
import time
import json
import numpy as np
import dask
from dask.distributed import Client, LocalCluster
import xarray as xr
import pandas as pd
from collections import defaultdict
from pathlib import Path


# Dask temp dir for PBS workers - adjust as needed for your environment
PBS_LOCAL_DIR = '/lustre/desc1/scratch/chiaweih/temp_dask'
PBS_LOG_DIR = '/lustre/desc1/scratch/chiaweih/temp_pbs'


def setup_logging(log_file="align_chunks.log"):
    """Configure logging to both console and a log file.

    Parameters
    ----------
    log_file : str
        Path to the log file.
    Returns
    -------
    str
        Full path to the configured log file.
    """

    if os.path.exists(log_file):
        os.remove(log_file)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # reset existing handlers to avoid duplicated messages on repeated runs
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logging.info("Logging initialized. Log file: %s", log_file)
    return log_file


def collect_matching_files(top_directory, pattern, exclude_pattern=None):
    """Collect all files under top_directory that match pattern, optionally excluding files that match exclude_pattern."""
    matched_files = []
    for dirpath, _, filenames in os.walk(top_directory):
        for filename in filenames:
            if Path(filename).match(pattern):
                # exclude_pattern is applied as a second filter after pattern matching
                #  this is per file scan and all exclude patterns are tested against the filename
                if exclude_pattern is None or not any(Path(filename).match(p) for p in exclude_pattern):
                    matched_files.append(os.path.join(dirpath, filename))
    return sorted(matched_files)


def create_dask_client(args):
    """Create a Dask client using local cluster or PBS cluster."""
    if args.dask_cluster == "local":
        cluster = LocalCluster(
            n_workers=args.num_workers,
            threads_per_worker=1,
            processes=True,
        )
        client = Client(cluster)
        logging.info(
            "Dask local cluster started | workers=%s threads/worker=1",
            args.num_workers,
        )
        return client, cluster

    try:
        from dask_jobqueue import PBSCluster
    except ImportError as exc:
        raise RuntimeError(
            "dask_jobqueue is required for --dask-cluster pbs. Install with: pip install dask-jobqueue"
        ) from exc

    cluster = PBSCluster(
        job_name='gdex-rechunk',
        cores=1,
        memory='4GiB',
        processes=1,
        account='P43713000',
        local_directory=PBS_LOCAL_DIR,
        log_directory=PBS_LOG_DIR,
        resource_spec='select=1:ncpus=1:mem=4GB',
        queue='gdex',
        walltime=args.walltime,
        interface='ext',
    )
    cluster.scale(jobs=args.num_workers)
    client = Client(cluster)
    logging.info(
        "Dask PBS cluster started | jobs=%s | fixed config (1 core, 4GiB, queue=gdex)",
        args.num_workers,
    )
    return client, cluster


def _extract_file_chunk_info(filepath, top_directory, file_index, exclude_variables=None):
    """
    Extract chunk/encoding info for all variables in one NetCDF file.
    
    Parameters:
    -----------
    filepath : str
        Path to the NetCDF file.
    top_directory : str
        Top-level directory for relative path calculation.
    file_index : int
        Index of the file in the list of files.
    exclude_variables : list or set, optional
        Variables to exclude from chunk/encoding extraction inside the NetCDF file.

    """
    relative_path = os.path.relpath(filepath, top_directory)
    records = []

    if exclude_variables is None:
        exclude_variables = set()
    else:
        exclude_variables = set(exclude_variables)


    try:
        ds = xr.open_dataset(filepath, decode_times=False, chunks={})
        for var_name in ds.data_vars:
            if var_name in exclude_variables:
                continue
            var = ds[var_name]

            if hasattr(var, 'encoding') and 'chunksizes' in var.encoding:
                try:
                    chunks = tuple(var.encoding['chunksizes'])
                except TypeError:
                    chunks = None
            else:
                chunks = None

            records.append(
                {
                    'var_name': var_name,
                    'file': relative_path,
                    'file_path': filepath,
                    'file_index': file_index,
                    'chunks': chunks,
                    'shape': var.shape,
                    'encoding': var.encoding,
                }
            )
        ds.close()
        return {'filepath': filepath, 'records': records, 'error': None}
    except Exception as e:
        return {'filepath': filepath, 'records': [], 'error': str(e)}


def check_chunk_consistency(file_paths, top_directory, client, exclude_variables=None):
    """
    Check for chunk size mismatches across NetCDF files.
    
    Parameters:
    -----------
    file_paths : list[str]
        List of file paths to check.
    top_directory : str
        Top-level directory used to compute relative file paths for reporting.
    client : dask.distributed.Client
        Dask client for parallel processing.
    exclude_variables : list or set, optional
        Variables to exclude from chunk consistency checks. 
        these are variable in the NetCDF files
    """
    files = sorted(file_paths)
    
    if not files:
        logging.warning("No files found matching pattern under: %s", top_directory)
        return
    if exclude_variables:
        logging.info("Excluding variables from chunk consistency checks: %s", exclude_variables)
    
    logging.info("Checking %s files for chunk consistency...", len(files))
    logging.info("%s", "=" * 80)
    
    # Store chunk info for each variable across all files (automatically creates an empty list for any new key)
    var_chunks = defaultdict(list)
    progress_interval = 100
    start_time = time.time()
    last_progress_time = start_time

    for i, filepath in enumerate(files, start=1):
        _extract_file_chunk_info(filepath, top_directory, i, exclude_variables=exclude_variables)
        
    delayed_tasks = [
        dask.delayed(_extract_file_chunk_info)(filepath, top_directory, i, exclude_variables=exclude_variables)
        for i, filepath in enumerate(files, start=1)
    ]

    processed = 0
    for batch_start in range(0, len(delayed_tasks), progress_interval):
        batch_tasks = delayed_tasks[batch_start:batch_start + progress_interval]
        futures = client.compute(batch_tasks)
        batch_results = client.gather(futures)

        for result in batch_results:
            if result['error']:
                logging.error("Error reading %s: %s", result['filepath'], result['error'])
                continue

            for record in result['records']:
                var_name = record.pop('var_name')
                var_chunks[var_name].append(record)

        processed += len(batch_tasks)
        if processed % progress_interval == 0 or processed == len(files):
            now = time.time()
            total_elapsed = now - start_time
            interval_elapsed = now - last_progress_time
            logging.info(
                "Progress [scan]: %s/%s files processed | elapsed %.1fs | last %s files %.1fs",
                processed,
                len(files),
                total_elapsed,
                len(batch_tasks),
                interval_elapsed,
            )
            last_progress_time = now
    
    # Analyze for mismatches
    mismatch_records = []
    
    for var_name, chunk_list in var_chunks.items():
        # Group by chunk size and count occurrences
        chunk_groups = defaultdict(list)
        for item in chunk_list:
            chunk_groups[item['chunks']].append(item)
        
        # Check if there are multiple different chunk patterns
        if len(chunk_groups) > 1:
            # Find the most common chunk size (excluding None) - this is our target
            valid_chunks = {k: v for k, v in chunk_groups.items() if k is not None}
            
            if valid_chunks:
                # Get the most common chunk pattern
                target_chunks = max(valid_chunks.items(), key=lambda x: len(x[1]))[0]
            else:
                target_chunks = None
            
            mismatch_records.append((var_name, chunk_groups, target_chunks))
            
            logging.info("  MISMATCH FOUND: %s", var_name)
            logging.info("   Unique chunk patterns: %s", len(chunk_groups))
            if target_chunks:
                logging.info(
                    "   Target chunks (most common): %s (%s files)",
                    target_chunks,
                    len(chunk_groups[target_chunks]),
                )
            
            # Display chunk patterns with counts
            for chunks, items in chunk_groups.items():
                marker = " ← TARGET" if chunks == target_chunks else ""
                logging.info("   Chunks %s: %s files%s", chunks, len(items), marker)
                # Show first 5 files with this chunking
                for item in items[:1]:
                    logging.info(
                        "      - File %s: %s (shape: %s)",
                        item['file_index'],
                        item['file'],
                        item['shape'],
                    )
                if len(items) > 5:
                    logging.info("      ... and %s more files", len(items) - 1)
    
    # Summary
    logging.info("%s", "=" * 80)
    logging.info("SUMMARY:")
    logging.info("  Total files checked: %s", len(files))
    logging.info("  Total variables: %s", len(var_chunks))
    logging.info("  Variables with chunk mismatches: %s", len(mismatch_records))
    
    if not mismatch_records:
        logging.info("✓ No chunk mismatches found!")
    else:
        logging.info(" Variables with mismatches:")
        for var_name, chunk_groups, target_chunks in mismatch_records:
            logging.info("    - %s", var_name)

    # Identify files to fix (match majority chunk size)
    df_files_to_fix = identify_files_to_rechunk(mismatch_records)
    
    return df_files_to_fix


def clean_encoding(encoding_dict):
    """
    Remove encoding parameters that are not supported by netCDF writer.
    
    Parameters:
    -----------
    encoding_dict : dict
        Original encoding dictionary
        
    Returns:
    --------
    dict: Cleaned encoding dictionary with only valid parameters
    """
    # Valid encoding parameters for netCDF
    valid_params = {
        'contiguous', 'compression_opts', '_FillValue', 'blosc_shuffle', 
        'zlib', 'complevel', 'endian', 'fletcher32', 'chunksizes', 
        'compression', 'quantize_mode', 'szip_coding', 'significant_digits', 
        'shuffle', 'szip_pixels_per_block', 'dtype'
    }
    
    cleaned = {}
    for key, value in encoding_dict.items():
        if key in valid_params:
            cleaned[key] = value
    
    return cleaned


def identify_files_to_rechunk(mismatches):
    """
    Identify which files need rechunking to match the most common chunk size.
    
    Parameters:
    -----------
    mismatches : list of tuples
        Each tuple is (var_name, chunk_groups, target_chunks) where:
        - chunk_groups is a dict mapping chunks to list of files
        - target_chunks is the most common chunk pattern (pre-computed)
    
    Returns:
    --------
    pd.DataFrame: DataFrame with columns ['variable', 'target_chunks', 'target_encoding', 'file']
    """
    rechunk_records = []
    
    # print("\n" + "=" * 80)
    # print("RECHUNKING RECOMMENDATIONS:")
    # print("=" * 80)
    
    for var_name, chunk_groups, target_chunks in mismatches:
        # Skip if no valid target chunks
        if target_chunks is None:
            logging.warning("⚠️  %s: No valid chunks found, skipping", var_name)
            continue

        majority_files = chunk_groups[target_chunks]

        # Files that need rechunking (have different chunks)
        files_to_fix = []
        for chunks, items in chunk_groups.items():
            if chunks != target_chunks:
                files_to_fix.extend(items)

        if files_to_fix:
            # Get encoding from a file that has the target chunks (use first majority file)
            target_encoding = clean_encoding(majority_files[0]['encoding'])

            # Add records to list
            for item in files_to_fix:
                rechunk_records.append({
                    'variable': var_name,
                    'target_chunks': target_chunks,
                    'target_encoding': target_encoding,  # Keep as dict object
                    'file': item['file'],
                    'file_path': item['file_path'],
                })

    # Create DataFrame
    df = pd.DataFrame(rechunk_records)
    return df


def _serialize_json_value(value):
    """Serialize python object to JSON string for parquet-safe storage."""
    if value is None or value is pd.NA:
        return None

    def _json_default(obj):
        if obj is pd.NA:
            return None
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.dtype):
            return str(obj)
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if hasattr(obj, 'item'):
            try:
                return obj.item()
            except Exception:
                pass
        return str(obj)

    return json.dumps(value, default=_json_default)


def _deserialize_json_value(value):
    """Deserialize JSON string back to python object."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    return json.loads(value)


def _normalize_encoding_for_netcdf(encoding):
    """Normalize encoding values to netCDF-writer compatible types."""
    normalized = dict(encoding)

    if '_FillValue' not in normalized:
        normalized['_FillValue'] = None

    chunksizes = normalized.get('chunksizes')
    if isinstance(chunksizes, list):
        normalized['chunksizes'] = tuple(chunksizes)
    elif isinstance(chunksizes, np.ndarray):
        normalized['chunksizes'] = tuple(chunksizes.tolist())

    return normalized


def write_rechunk_plan_parquet(df_rechunk, parquet_path):
    """Write rechunk plan DataFrame to parquet with JSON-safe object columns."""
    output_dir = os.path.dirname(os.path.abspath(parquet_path))
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df_out = df_rechunk.copy()
    df_out['target_encoding'] = df_out['target_encoding'].apply(_serialize_json_value)
    df_out['target_chunks'] = df_out['target_chunks'].apply(_serialize_json_value)
    df_out.to_parquet(parquet_path, index=False)
    logging.info("Saved rechunk plan parquet: %s", parquet_path)


def read_rechunk_plan_parquet(parquet_path):
    """Read rechunk plan DataFrame from parquet and restore object columns."""
    df_in = pd.read_parquet(parquet_path)
    if df_in.empty:
        return df_in

    if 'target_encoding' in df_in.columns:
        df_in['target_encoding'] = df_in['target_encoding'].apply(_deserialize_json_value)
    if 'target_chunks' in df_in.columns:
        df_in['target_chunks'] = df_in['target_chunks'].apply(_deserialize_json_value)
    return df_in


def _rechunk_single_file(file_job):
    """Rechunk one file and return execution metadata."""
    input_filepath = file_job['input_filepath']
    output_filepath = file_job['output_filepath']
    relative_path = file_job['relative_path']
    operations = file_job['operations']

    if os.path.exists(output_filepath):
        logging.info("Skipping (already exists): %s", output_filepath)
        return {
            'relative_path': relative_path,
            'output_filepath': output_filepath,
            'applied': [],
            'missing': [],
            'error': None,
        }

    try:
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        ds = xr.open_dataset(input_filepath, decode_times=False, chunks={})

        applied = []
        missing = []
        file_encodings = {}
        for op in operations:
            var_name = op['variable']
            target_chunks = op['target_chunks']
            if isinstance(target_chunks, list):
                target_chunks = tuple(target_chunks)
            if var_name in ds.data_vars:
                enc = _normalize_encoding_for_netcdf(op['target_encoding'])
                chunksizes = enc.get('chunksizes')
                if chunksizes is not None:
                    enc['contiguous'] = False
                    chunks_dict = dict(zip(ds[var_name].dims, chunksizes))
                    ds[var_name] = ds[var_name].chunk(chunks_dict)

                file_encodings[var_name] = enc
                applied.append((var_name, target_chunks))
            else:
                missing.append(var_name)

        ds.to_netcdf(output_filepath, encoding=file_encodings)
        logging.info("Saved rechunked file: %s", output_filepath)
        ds.close()
        return {
            'relative_path': relative_path,
            'output_filepath': output_filepath,
            'applied': applied,
            'missing': missing,
            'error': None,
        }
    except Exception as e:
        return {
            'relative_path': relative_path,
            'output_filepath': output_filepath,
            'applied': [],
            'missing': [],
            'error': str(e),
        }


def execute_rechunk(rechunk_df, top_directory, output_directory, client):
    """
    Execute rechunking by rewriting NetCDF files with target encoding.
    Groups by file and applies all necessary encoding changes in one to_netcdf call.
    
    Parameters:
    -----------
    rechunk_df : pd.DataFrame
        DataFrame with columns ['variable', 'target_chunks', 'target_encoding', 'file', 'file_path']
    top_directory : str
        Top-level data directory used to preserve relative structure in outputs.
    output_directory : str
        Directory where the rechunked files will be saved
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    
    logging.info("%s", "=" * 80)
    logging.info("EXECUTING RECHUNK")
    logging.info("%s", "=" * 80)
    logging.info("Top directory: %s", top_directory)
    logging.info("Output directory: %s", output_directory)
    
    # Build file jobs for parallel execution
    file_jobs = []
    grouped = rechunk_df.groupby('file_path')
    for input_filepath, group in grouped:
        relative_path = os.path.relpath(input_filepath, top_directory)
        output_filepath = os.path.join(output_directory, relative_path)
        if os.path.exists(output_filepath):
            logging.info("Skipping (already exists): %s", output_filepath)
            continue
        operations = []
        for _, row in group.iterrows():
            operations.append(
                {
                    'variable': row['variable'],
                    'target_encoding': row['target_encoding'],
                    'target_chunks': row['target_chunks'],
                }
            )
        file_jobs.append(
            {
                'input_filepath': input_filepath,
                'output_filepath': output_filepath,
                'relative_path': relative_path,
                'operations': operations,
            }
        )

    total_files = len(file_jobs)
    progress_interval = 100
    start_time = time.time()
    last_progress_time = start_time

    delayed_tasks = [dask.delayed(_rechunk_single_file)(job) for job in file_jobs]
    processed = 0
    for batch_start in range(0, len(delayed_tasks), progress_interval):
        batch_tasks = delayed_tasks[batch_start:batch_start + progress_interval]
        futures = client.compute(batch_tasks)
        batch_results = client.gather(futures)

        for result in batch_results:
            if result['error']:
                logging.error("    [%s] %s", result['relative_path'], result['error'])
                continue
            if not result['applied'] and not result['missing']:
                logging.info("    No changes needed for: %s", result['relative_path'])
                continue
            
            logging.info("    Saved to: %s", result['output_filepath'])
            for var_name, target_chunks in result['applied']:
                logging.info("   - %s: chunks %s", var_name, target_chunks)
            for var_name in result['missing']:
                logging.warning("     Variable %s not found in file", var_name)

        processed += len(batch_tasks)
        if processed % progress_interval == 0 or processed == total_files:
            now = time.time()
            total_elapsed = now - start_time
            interval_elapsed = now - last_progress_time
            logging.info(
                "Progress [execute]: %s/%s files written | elapsed %.1fs | last %s files %.1fs",
                processed,
                total_files,
                total_elapsed,
                len(batch_tasks),
                interval_elapsed,
            )
            last_progress_time = now

    logging.info("%s", "=" * 80)
    logging.info("RECHUNKING COMPLETE")
    logging.info("%s", "=" * 80)


def _add_common_args(p):
    """
    Add common command line arguments for both 'plan' and 'execute' subcommands.
    """
    p.add_argument(
        "top_directory",
        type=str,
        help="Top-level directory to recursively scan for NetCDF files.",
    )
    p.add_argument(
        "--log-file",
        type=str,
        default="align_chunks.log",
        help="Log filename (default: align_chunks.log).",
    )
    p.add_argument(
        "--dask-cluster",
        type=str,
        choices=["local", "pbs"],
        default="pbs",
        help="Dask cluster backend (default: pbs).",
    )
    p.add_argument(
        "--num-workers",
        type=int,
        default=8,
        help="Number of local workers or PBS jobs to scale to (default: 8).",
    )
    p.add_argument(
        "--walltime",
        type=str,
        default="24:00:00",
        help="Walltime for PBS jobs in HH:MM:SS format (default: 24:00:00).",
    )

    return p

def _get_parser():
    """Create command line parser for chunk consistency check and rechunking."""
    parser = argparse.ArgumentParser(
        description=(
            "Walk a top-level directory, detect NetCDF chunk-size inconsistencies, "
            "and rechunk files using the majority chunk pattern. "
            "Two stages: 'plan' scans and writes a parquet plan; 'execute' applies the plan."
        )
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Subparser for the planning stage
    plan_parser = subparsers.add_parser(
        'plan',
        help="Scan files for chunk inconsistencies and write a rechunk plan parquet.",
    )
    plan_parser = _add_common_args(plan_parser)
    plan_parser.add_argument(
        "--pattern",
        type=str,
        default="*.nc",
        help="Filename glob pattern to use within each directory (default: *.nc).",
    )
    plan_parser.add_argument(
        "--exclude-pattern",
        nargs='+',
        default=None,
        help="Filename glob pattern to exclude from the results of --pattern. Applied as a second filter after --pattern.",
    )
    plan_parser.add_argument(
        "--output-parquet",
        type=str,
        required=True,
        help="Path to write the rechunk plan parquet file.",
    )
    plan_parser.add_argument(
        "--exclude-variables",
        nargs='+',
        default=None,
        help=(
            "List of variable names to exclude from chunk consistency checks. "
            "Useful for variables that are known to have different chunking and should be ignored."
        ),
    )   

    # Subparser for the execution stage
    execute_parser = subparsers.add_parser(
        'execute',
        help="Execute rechunking from an existing rechunk plan parquet.",
    )
    execute_parser = _add_common_args(execute_parser)
    execute_parser.add_argument(
        "--input-parquet",
        type=str,
        required=True,
        help="Path to the rechunk plan parquet file produced by the plan stage.",
    )
    execute_parser.add_argument(
        "--output-directory",
        type=str,
        required=True,
        help=(
            "Root output directory for rewritten files. "
            "Subdirectory structure relative to top_directory is preserved."
        ),
    )

    return parser


def main():
    """CLI entrypoint."""
    parser = _get_parser()
    args = parser.parse_args()

    top_directory = os.path.abspath(args.top_directory)
    if not os.path.isdir(top_directory):
        parser.error(f'top_directory does not exist or is not a directory: {top_directory}')

    setup_logging(log_file=args.log_file)

    client = None
    cluster = None
    try:
        client, cluster = create_dask_client(args)

        if args.command == 'plan':
            matched_files = collect_matching_files(top_directory, args.pattern, args.exclude_pattern)
            logging.info(
                "Matched %s files under %s with pattern %s%s",
                len(matched_files),
                top_directory,
                args.pattern,
                f" (excluding: {args.exclude_pattern})" if args.exclude_pattern else "",
            )

            df_rechunk = check_chunk_consistency(matched_files, top_directory, client, exclude_variables=args.exclude_variables)

            if df_rechunk is None or df_rechunk.empty:
                logging.info("✓ No rechunking needed!")
            else:
                write_rechunk_plan_parquet(df_rechunk, args.output_parquet)
                logging.info("Total files requiring rechunk: %s", df_rechunk['file_path'].nunique())

        elif args.command == 'execute':
            output_directory = os.path.abspath(args.output_directory)

            if not os.path.exists(args.input_parquet):
                parser.error(f'--input-parquet file not found: {args.input_parquet}')

            df_rechunk = read_rechunk_plan_parquet(args.input_parquet)

            if df_rechunk is None or df_rechunk.empty:
                logging.warning("✓ Rechunk plan empty!")
            else:
                logging.info("Total files requiring rechunk: %s", df_rechunk['file_path'].nunique())
                execute_rechunk(df_rechunk, top_directory, output_directory, client)

        logging.info('%s', '##################################################################################')
        logging.info('DONE')
    finally:
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()


if __name__ == "__main__":
    main()