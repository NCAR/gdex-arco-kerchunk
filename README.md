# GDEX ARCO Kerchunk

Tool suite for creating Kerchunk reference files and managing NetCDF chunk consistency for GDEX (Geoscience Data Exchange) data holdings at NCAR.

---

## Overview

This repository provides two main CLI tools:

1. **create_kerchunk.py** - Generate Kerchunk reference files for cloud-optimized data access
2. **align_chunks.py** - Detect and fix NetCDF chunk-size inconsistencies across files

Both tools support distributed processing with Dask for large-scale data operations.

---

## create_kerchunk.py

### Purpose

Create Kerchunk reference files from GDEX NetCDF data holdings. Supports both individual sidecar files and combined aggregated references, with optional remote URL conversion for cloud access.

### Usage

```bash
python src/create_kerchunk.py --action <combine|sidecar> --directory <directory> [OPTIONS]
```

### Options & Arguments

#### Required Arguments

```
--action {combine|sidecar}, -a {combine|sidecar}
    Specify the operation mode:
    - combine: Aggregate multiple files into single reference
    - sidecar: Create individual reference file per data file

--directory <directory>, -d <directory>
    Root directory to scan for NetCDF files
```

#### Output Options

```
--output_directory <directory>, -o <directory>
    Directory for output files (default: current directory)

--filename <filename>, -f <filename>
    Output filename for combined references (default: auto-generated)

--output_format {json|parquet}, -of {json|parquet}
    Output format for combined references (default: json)
    Note: Parquet support is experimental
```

#### File Selection Options

```
--extensions <ext> [ext ...], -e <ext> [ext ...]
    Process only files with these extensions (e.g., nc grib)

--regex <pattern>, -r <pattern>
    Include files matching this regex pattern (full path)
    Example: "wrf2d_.*\.nc"

--regex_exclude <pattern>, -re <pattern>
    Exclude files matching this regex pattern (full path)
    Example: ".*_backup\.nc$"
```

#### Variable Selection Options

```
--variables <var> [var ...], -v <var> [var ...]
    Include only specific variables (space-separated)
    Example: temperature precipitation humidity

--exclude_variables <var> [var ...], -ev <var> [var ...]
    Exclude specific variables from combined reference
    Example: XTIME XLONG XLAT
```

#### Processing Options

```
--cluster {PBS|single|local}, -c {PBS|single|local}
    Dask cluster type (default: local):
    - PBS: PBSCluster (NCAR HPC, uses gdex queue)
    - single: Single-threaded
    - local: LocalCluster (multi-process, all CPUs)

--num_processes <int>, -n <int>
    Number of Dask workers/jobs (default: 8)

--dry_run, -dr
    Preview processing without creating files

--make_remote, -mr
    Create additional remote-accessible copies with GDEX URLs
```

### Use Cases & Examples

#### Use Case 1: Create Individual Sidecar Files

Create one reference file per NetCDF file in a directory structure.

```bash
python src/create_kerchunk.py \
    --action sidecar \
    --directory /glade/campaign/collections/gdex/data/d640000/bnd_ocean/194907 \
    --output_directory ./kerchunk_refs \
    --extensions nc
```

**Output:** `./kerchunk_refs/194907/file1.nc.json`, `./kerchunk_refs/194907/file2.nc.json`, etc.

---

#### Use Case 2: Combine All Files into Single Reference

Aggregate all matching files into one combined reference for easier management.

```bash
python src/create_kerchunk.py \
    --action combine \
    --directory /glade/campaign/collections/gdex/data/d640000/bnd_ocean/194907 \
    --output_directory ./output \
    --extensions nc \
    --filename bnd_ocean_combined.json
```

**Output:** `./output/bnd_ocean_combined.json` with all files aggregated by time

---

#### Use Case 3: Filter Files by Pattern

Combine only files matching a specific pattern, excluding backups.

```bash
python src/create_kerchunk.py \
    --action combine \
    --directory /glade/campaign/collections/gdex/data/d640000 \
    --output_directory ./output \
    --extensions nc \
    --regex "wrf2d_.*\.nc" \
    --regex_exclude ".*_backup\.nc$" \
    --filename wrf2d_combined.json
```

**Output:** Combined reference containing only `wrf2d_*.nc` files (excluding backups)

---

#### Use Case 4: Select Specific Variables Only

Reduce reference file size by including only variables of interest.

```bash
python src/create_kerchunk.py \
    --action combine \
    --directory /glade/campaign/collections/gdex/data/d640000 \
    --output_directory ./output \
    --extensions nc \
    --variables T Q U V PSFC \
    --filename atmosphere_only.json
```

**Output:** Reference containing only temperature, humidity, wind, and pressure variables

---

#### Use Case 5: Exclude Coordinate Variables

Create reference without spatial/temporal coordinates to reduce size.

```bash
python src/create_kerchunk.py \
    --action combine \
    --directory /glade/campaign/collections/gdex/data/d640000/bnd_ocean \
    --output_directory ./output \
    --extensions nc \
    --exclude_variables XTIME XLONG XLAT XLONG_U XLAT_U XLONG_V XLAT_V \
    --filename data_only.json
```

**Output:** Reference with only data variables (coordinates available separately)

---

#### Use Case 6: Create Remote-Accessible References

Generate references with GDEX URLs for cloud-native data access.

```bash
python src/create_kerchunk.py \
    --action combine \
    --directory /glade/campaign/collections/gdex/data/d640000 \
    --output_directory ./output \
    --extensions nc \
    --filename d640000.json \
    --make_remote
```

**Output:** 
- `./output/d640000.json` (local file paths)
- `./output/d640000-remote-https.json` (GDEX HTTPS URLs)

---

#### Use Case 7: Parquet Format with PBS Cluster

Combine files to parquet format using HPC cluster for large datasets.

```bash
python src/create_kerchunk.py \
    --action combine \
    --directory /glade/campaign/collections/gdex/data/d640000 \
    --output_directory ./output \
    --extensions nc \
    --output_format parquet \
    --filename d640000.parq \
    --cluster PBS \
    --num_processes 32
```

**Output:** `./output/d640000.parq` (Parquet reference file processed on 32 HPC nodes)

---

#### Use Case 8: Dry Run Preview

Preview what files will be processed without creating references.

```bash
python src/create_kerchunk.py \
    --action combine \
    --directory /glade/campaign/collections/gdex/data/d640000 \
    --extensions nc \
    --regex "wrf2d_.*\.nc" \
    --dry_run
```

**Output:** Shows number of files and structure without processing

---

### Command-Line Reference

| Argument | Short | Type | Default | Description |
|----------|-------|------|---------|-------------|
| action | -a | combine\|sidecar | required | Operation mode |
| directory | -d | str | required | Root directory to scan |
| output_directory | -o | str | . | Output directory |
| filename | -f | str | auto | Output filename |
| extensions | -e | str[] | [] | File extensions to process |
| regex | -r | str | None | Include pattern (full path) |
| regex_exclude | -re | str | None | Exclude pattern (full path) |
| variables | -v | str[] | [] | Variables to include |
| exclude_variables | -ev | str[] | [] | Variables to exclude |
| cluster | -c | PBS\|single\|local | local | Dask cluster type |
| num_processes | -n | int | 8 | Number of workers |
| output_format | -of | json\|parquet | json | Output format |
| dry_run | -dr | flag | False | Preview mode |
| make_remote | -mr | flag | False | Create remote URLs |

---

## align_chunks.py

### Purpose

Detect NetCDF chunk-size inconsistencies across a directory of files and automatically rechunk them to match the majority pattern. Uses a two-stage workflow: plan (scan + identify) and execute (rechunk + rewrite).

### Usage

```bash
# Stage 1: Plan
python src/align_chunks.py plan <top_directory> [OPTIONS]

# Stage 2: Execute
python src/align_chunks.py execute <top_directory> [OPTIONS]
```

### Options & Arguments

#### Common Arguments (Both Stages)

```
<top_directory>
    Root directory containing NetCDF files to process

--log_file <path>, --log-file <path>
    Log filename (default: align_chunks.log)

--dask_cluster {local|pbs}, --dask-cluster {local|pbs}
    Dask cluster backend (default: pbs):
    - local: LocalCluster (multiprocessing)
    - pbs: PBSCluster (NCAR HPC)

--num_workers <int>, --num-workers <int>
    Number of workers/jobs (default: 8)

--walltime <HH:MM:SS>
    Walltime for PBS jobs (default: 24:00:00)
```

#### Plan Stage Options

```
--pattern <glob>
    Filename glob pattern to match (default: *.nc)
    Applied to filename only (not full path)
    Example: "wrf2d_*.nc"

--exclude_pattern <glob> [...], --exclude-pattern <glob> [...]
    Filename patterns to exclude (default: None)
    Applied as second filter after --pattern
    Example: "*_backup.nc" "*_old.nc"

--exclude_variables <var> [...], --exclude-variables <var> [...]
    Variable names to exclude from consistency checks (default: None)
    Example: XTIME REFERENCE_TIME

--output_parquet <path>, --output-parquet <path>
    Path to write rechunk plan parquet file (required)
    Example: ./rechunk_plan.parq
```

#### Execute Stage Options

```
--input_parquet <path>, --input-parquet <path>
    Path to rechunk plan parquet from plan stage (required)
    Example: ./rechunk_plan.parq

--output_directory <path>, --output-directory <path>
    Output directory for rechunked files (required)
    Subdirectory structure relative to top_directory is preserved
    Example: ./rechunked_data
```

### Use Cases & Examples

#### Use Case 1: Scan for Chunk Mismatches

Identify chunk inconsistencies without making changes.

```bash
python src/align_chunks.py plan /gdex/data/d651007 \
    --output-parquet ./rechunk_plan.parq
```

**Output:** 
- `./rechunk_plan.parq` (detailed plan with all inconsistencies)
- `align_chunks.log` (human-readable report of mismatches)

**Log Output Example:**
```
MISMATCH FOUND: temperature
  Unique chunk patterns: 3
  Target chunks (most common): (10, 20, 30) (450 files) ← TARGET
  Chunks (10, 20, 30): 450 files
  Chunks (10, 20, 50): 30 files
  Chunks (5, 20, 30): 20 files
```

---

#### Use Case 2: Plan with File Pattern Filtering

Scan only specific files, excluding backups and old versions.

```bash
python src/align_chunks.py plan /gdex/data/d651007 \
    --pattern "wrf2d_*.nc" \
    --exclude-pattern "*_backup.nc" "*_v1.nc" \
    --output-parquet ./rechunk_plan.parq \
    --log-file ./plan_scan.log
```

**Output:** Plan containing only `wrf2d_*.nc` files (excluding backups and v1)

---

#### Use Case 3: Exclude Coordinate Variables from Checks

Skip checking coordinates that are known to differ.

```bash
python src/align_chunks.py plan /gdex/data/d651007 \
    --exclude-variables XTIME REFERENCE_TIME lon lat \
    --output-parquet ./rechunk_plan.parq
```

**Output:** Plan checking only data variables, ignoring time and spatial coords

---

#### Use Case 4: Execute Rechunking

Apply the plan to rechunk all identified files.

```bash
python src/align_chunks.py execute /gdex/data/d651007 \
    --input-parquet ./rechunk_plan.parq \
    --output-directory ./d651007_rechunked
```

**Output:** Rechunked NetCDF files in `./d651007_rechunked` with directory structure preserved

---

#### Use Case 5: Execute with Local Cluster

Rechunk using local multiprocessing instead of PBS.

```bash
python src/align_chunks.py execute /gdex/data/d651007 \
    --input-parquet ./rechunk_plan.parq \
    --output-directory ./d651007_rechunked \
    --dask-cluster local \
    --num-workers 4
```

**Output:** Rechunked files processed on 4 local workers

---

#### Use Case 6: Execute with Custom Walltime

Run execute stage with extended walltime on PBS cluster.

```bash
python src/align_chunks.py execute /gdex/data/d651007 \
    --input-parquet ./rechunk_plan.parq \
    --output-directory ./d651007_rechunked \
    --dask-cluster pbs \
    --num-workers 32 \
    --walltime 72:00:00
```

**Output:** Rechunking processed on 32 HPC nodes with 72-hour walltime

---

#### Use Case 7: Complete Workflow - Plan and Execute

Full pipeline from detection to rechunking with large dataset.

```bash
# Step 1: Scan for mismatches (with filtering)
python src/align_chunks.py plan /gdex/data/d651007 \
    --pattern "wrf2d_*.nc" \
    --exclude-pattern "*_backup.nc" \
    --exclude-variables XTIME \
    --output-parquet /scratch/rechunk_plan.parq \
    --log-file /scratch/plan_d651007.log \
    --dask-cluster pbs \
    --num-workers 32

# Step 2: Review plan (check log for mismatches)
cat /scratch/plan_d651007.log

# Step 3: Execute rechunking
python src/align_chunks.py execute /gdex/data/d651007 \
    --input-parquet /scratch/rechunk_plan.parq \
    --output-directory /scratch/d651007_rechunked \
    --log-file /scratch/execute_d651007.log \
    --dask-cluster pbs \
    --num-workers 32 \
    --walltime 48:00:00
```

**Output:**
- Plan log showing all mismatches found
- Rechunked NetCDF files in output directory
- Execute log showing all operations performed

---

#### Use Case 8: Verify Results

Check rechunked files for consistency after execution.

```bash
# Re-run plan stage on output directory to verify all fixed
python src/align_chunks.py plan /scratch/d651007_rechunked \
    --pattern "wrf2d_*.nc" \
    --output-parquet /scratch/verify_plan.parq \
    --log-file /scratch/verify.log

# Check log - should show "No chunk mismatches found!"
cat /scratch/verify.log
```

**Output:** Verification log confirming all chunks are now consistent

---

### Command-Line Reference

| Argument | Type | Default | Stage | Description |
|----------|------|---------|-------|-------------|
| top_directory | str | required | Both | Root directory to process |
| log_file | str | align_chunks.log | Both | Log output file |
| dask_cluster | local\|pbs | pbs | Both | Cluster backend |
| num_workers | int | 8 | Both | Number of workers |
| walltime | HH:MM:SS | 24:00:00 | Both | PBS job walltime |
| pattern | str | *.nc | Plan | File glob pattern |
| exclude_pattern | str[] | None | Plan | Exclude patterns |
| exclude_variables | str[] | None | Plan | Variables to skip |
| output_parquet | str | required | Plan | Plan output file |
| input_parquet | str | required | Execute | Plan input file |
| output_directory | str | required | Execute | Rechunked output dir |

---

## Repository Structure

```
├── src/
│   ├── create_kerchunk.py       # Create Kerchunk references
│   ├── align_chunks.py          # Detect & fix chunk inconsistencies
│   ├── create_kerchunk_grib.py  # GRIB format support
│   ├── convert_ref_file_loc.py  # Convert local → remote URLs
│   ├── separate_kerchunk.py     # Split combined references
│   └── convert_chunks.py        # Modify chunk sizes
├── examples/                    # Usage examples & scripts
├── test/                        # Tests & validation
└── README.md                    # This file
```

---

## Key Features

### Distributed Processing
- **PBS Cluster**: Automatic job scheduling on NCAR HPC (gdex queue)
- **Local Cluster**: Multiprocessing on single machine
- **Single-threaded**: Debug mode

### Memory Management
- Batch processing (5000 files for create_kerchunk, 100 files for align_chunks)
- Real-time memory monitoring
- Intermediate progress reporting

### Pattern Matching
- **create_kerchunk.py**: Full-path regex matching
- **align_chunks.py**: Filename glob patterns

### Output Formats
- JSON (default, fully supported)
- Parquet (experimental for create_kerchunk)

---

## Important Notes

- **Variable Names**: Case sensitive
- **Parquet Format**: Experimental support, use JSON for production
- **Time Variable**: Automatically detected; manually check if detection fails
- **Remote URLs**: `--make_remote` creates separate reference files with GDEX URLs
- **Chunk Majority Rule**: align_chunks uses most common pattern as target
- **Directory Structure**: Preserved in all operations

---

## GDEX Integration

- **Data Source**: `/glade/campaign/collections/gdex/data/`
- **Remote Protocols**: HTTPS (https://data.gdex.ucar.edu) and OSDF

