#!/usr/bin/env python3
"""
parquet_endpoints_fastparquet.py - same tool as parquet_endpoints.py, but built
on fastparquet + pandas instead of pyarrow.

Inspect or rewrite the endpoint prefixes in a kerchunk parquet reference store
(a folder of <variable>/refs.N.parq files with columns path, offset, size, raw).
Only `path` is touched.

Examples
--------
  python parquet_endpoints.py show ./refs
  python parquet_endpoints.py replace ./refs --old /gdex/data/ --new https://data.gdex.ucar.edu/ --dry-run
  python parquet_endpoints.py replace ./refs --old /gdex/data/ --new https://data.gdex.ucar.edu/ --output ./refs_https
  python parquet_endpoints.py replace ./refs --old /gdex/data/ --new https://data.gdex.ucar.edu/ --in-place

Only values that START with --old change; end --old with "/" so that
/gdex/data/ does not also match /gdex/database/. To handle two source
prefixes, run replace once per prefix.

"""
import argparse
import os
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

import fastparquet as fp
import pandas as pd
from fastparquet.parquet_thrift import CompressionCodec

COLUMN = "path"
EXTENSIONS = (".parq", ".parquet")


def find_files(root: Path):
    return sorted(p for p in root.rglob("*")
                  if p.is_file() and p.suffix in EXTENSIONS)


def prefix_of(value: str, depth: int) -> str:
    """scheme://host plus the first `depth` path segments."""
    parts = urlsplit(value)
    segs = [s for s in parts.path.split("/") if s]
    kept = "/".join(segs[:depth])
    if parts.scheme:
        head = f"{parts.scheme}://{parts.netloc}"
        return f"{head}/{kept}" if kept else head
    return ("/" if value.startswith("/") else "") + kept


def open_file(f: Path):
    pf = fp.ParquetFile(str(f))
    if COLUMN not in pf.columns:
        print(f"warning: no '{COLUMN}' column in {f} -- skipped", file=sys.stderr)
        return None
    return pf


# ---------------------------------------------------------------- show
def cmd_show(args):
    root = Path(args.path)
    files = find_files(root)
    if not files:
        sys.exit(f"No .parq/.parquet files found under {root}")

    counts = Counter()
    null_rows = 0
    for f in files:
        pf = open_file(f)
        if pf is None:
            continue
        col = pf.to_pandas(columns=[COLUMN])[COLUMN].astype(object)
        for value, n in col.value_counts(dropna=False).items():
            if pd.isna(value):
                null_rows += int(n)
            else:
                counts[prefix_of(value, args.depth)] += int(n)

    print(f"{len(files)} files, {sum(counts.values()) + null_rows} rows, "
          f"{len(counts)} distinct prefixes (depth {args.depth}):")
    for value, n in counts.most_common():
        print(f"{n:>12}  {value}")
    if null_rows:
        print(f"\n{null_rows:>12}  rows with no path (inline data, empty rows or "
              f"padding); not rewritten")


# ------------------------------------------------------------- replace
def rewrite_file(src, dst, old, new, dry_run):
    pf = open_file(src)
    if pf is None:
        return 0, False
    df = pf.to_pandas()
    was_categorical = isinstance(df[COLUMN].dtype, pd.CategoricalDtype)
    s = df[COLUMN].astype(object)

    mask = s.str.startswith(old, na=False)
    n = int(mask.sum())
    if dry_run or n == 0:
        return n, False

    s.loc[mask] = new + s[mask].str[len(old):]
    df[COLUMN] = s.astype("category") if was_categorical else s

    codec = (pf.fmd.row_groups[0].columns[0].meta_data.codec
             if pf.fmd.row_groups else 0)
    compression = CompressionCodec._VALUES_TO_NAMES.get(codec, "SNAPPY")

    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(suffix=".tmp", dir=dst.parent)
    os.close(fd)
    try:
        fp.write(tmp, df, compression=compression)
        os.replace(tmp, dst)      # atomic swap
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return n, True


def cmd_replace(args):
    root = Path(args.path).resolve()
    if not root.is_dir():
        sys.exit(f"{root} is not a directory")
    if sum([bool(args.output), args.in_place, args.dry_run]) != 1:
        sys.exit("Choose exactly one of --output DIR, --in-place, or --dry-run")
    if not args.old:
        sys.exit("--old must be non-empty")

    files = find_files(root)
    if not files:
        sys.exit(f"No .parq/.parquet files found under {root}")

    out_root = Path(args.output).resolve() if args.output else None
    if out_root:
        if out_root == root or root in out_root.parents:
            sys.exit("--output must not be inside the source folder")
        if out_root.exists() and any(out_root.iterdir()):
            sys.exit(f"{out_root} exists and is not empty")

    total = changed_files = 0
    for src in files:
        rel = src.relative_to(root)
        dst = (out_root / rel) if out_root else src
        n, wrote = rewrite_file(src, dst, args.old, args.new, args.dry_run)
        total += n
        changed_files += bool(n)
        if out_root and not wrote and not args.dry_run:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)          # unchanged file: copy as-is
        if n:
            print(f"{'would change' if args.dry_run else 'changed'} "
                  f"{n:>9} rows  {rel}")

    if out_root:                            # .zmetadata and anything else
        done = set(files)
        for p in root.rglob("*"):
            if p.is_file() and p not in done:
                dst = out_root / p.relative_to(root)
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(p, dst)

    print(f"\n{total} rows in {changed_files}/{len(files)} files "
          f"{'would be ' if args.dry_run else ''}updated.")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("show", help="show endpoint prefixes with row counts")
    s.add_argument("path")
    s.add_argument("--depth", type=int, default=3,
                   help="path segments kept after scheme+host (default 3)")
    s.set_defaults(func=cmd_show)

    r = sub.add_parser("replace", help="swap an endpoint prefix")
    r.add_argument("path")
    r.add_argument("--old", required=True, help="existing prefix")
    r.add_argument("--new", required=True, help="replacement prefix")
    r.add_argument("--output", help="write a new store here")
    r.add_argument("--in-place", action="store_true", help="modify existing files")
    r.add_argument("--dry-run", action="store_true", help="report only")
    r.set_defaults(func=cmd_replace)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
