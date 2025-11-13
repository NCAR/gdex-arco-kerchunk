---
title: Introduction
date: 2025-11-05
---

# Introduction

Welcome to the **GDEX ARCO Kerchunk** documentation. This project provides tools and scripts to generate Kerchunk reference files and associated metadata, enabling efficient, cloud‑optimized access to datasets served on NCAR's GDEX infrastructure.

:::{warning} Important Notice
This documentation is under active development and is intended primarily for internal use by the GDEX team. It is being prepared to align with open‑science and open‑data principles, promoting transparency and reproducibility of the data‑processing pipeline.
:::

## What is GDEX ARCO Kerchunk?
ARCO (Analysis‑Ready Cloud‑Optimized) describes datasets and workflows prepared for efficient analysis in cloud or object‑store environments. The aim is to make data discoverable and directly usable without heavy preprocessing or repeated full‑downloads.

`kerchunk` is a lightweight approach that preserves the original file format (e.g., netCDF) by creating a JSON “reference” that maps logical array chunks to byte ranges inside the original files. User (via fsspec/xarray or Zarr-compatible tools) use that reference to read only the needed chunks over HTTP/S3 as if the data were stored chunk‑by‑chunk like a native Zarr store. Benefits include:
- No full data conversion or duplicate storage
- Efficient partial reads and reduced I/O and egress costs
- Compatibility with existing file formats and cloud protocols
- Easy integration into cataloging and access workflows (e.g., intake‑ESM catalog)

## Key Features
### 🛠️ Custom Kerchunk Reference Generation

The script `src/create_kerchunk.py` produces Kerchunk reference files for datasets and offers flexible configuration to match different workflows. You can create either a single aggregated (combined) reference that maps many data files into one logical store, or individual reference files per source file.

Modes
- Combined reference
    - Maps multiple input files into a single Kerchunk JSON that behaves like one dataset.
    - Best when users want to open many files together (simpler cataloging and access).
    - Slightly larger download for the reference but reduces per-file management overhead.
- Per-file (single) reference
    - Produces one reference JSON per source file.
    - Smaller, faster-to-fetch references — useful on low-bandwidth connections or when accessing a few files at a time.
    - Easier to update/incrementally regenerate.

Use combined references when you need a single logical view of many files; use per-file references when minimizing reference size and fetch time is the priority.

### 🌐 Multiple Access Methods
Generated kerchunk reference files support three access patterns :
- **POSIX**: Direct filesystem access for NCAR HPC users
- **HTTPS**: Web-based access for remote users
- **OSDF**: Distributed access through Open Science Data Federation


## Quick Start

Generate a basic catalog:

```bash
python src/create_kerchunk.py 
    --action combine 
    --directory /path/to/input_directory 
    --output_directory /path/to/output_directory 
    --extensions <data source file format> 
    --filename <output_filename> 
    --output_format <parquet|json|zarr> 
    [--make_remote]
```


For comprehensive usage examples:
- **NCAR HPC users**: [gdex-examples](https://ncar.github.io/gdex-examples/)
- **OSDF users**: [osdf_examples](https://ncar.github.io/osdf_examples/)

## Repository Structure

- **`src/`** - Core kerchunk reference generation tools
- **`examples/`** - Python script examples for generating dataset catalog
- **`test/`** - Test scripts and validation tools

## Content

This documentation provides:
1. Understanding the kerchunk reference file generation process
2. Accessing generated kerchunk file through different methods
