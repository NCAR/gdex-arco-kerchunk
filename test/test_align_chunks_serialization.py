#!/usr/bin/env python

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# load align_chunks module from src/ directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))
from align_chunks import read_rechunk_plan_parquet, write_rechunk_plan_parquet


def test_rechunk_plan_parquet_roundtrip_with_dtype_values(tmp_path):
    parquet_path = tmp_path / "mock_rechunk_plan.parquet"

    mock_df = pd.DataFrame(
        [
            {
                "variable": "T2",
                "target_chunks": (1, 100, 100),
                "target_encoding": {
                    "chunksizes": (1, 17, 339, 456),
                    "fletcher32": False,
                    "shuffle": True,
                    "preferred_chunks": {
                        "Time": 1,
                        "bottom_top": 17,
                        "south_north": 339,
                        "west_east": 456,
                    },
                    "zlib": True,
                    "complevel": np.int32(1),
                    "source": "/gdex/data/d559000/wy1980/197910/wrf3d_d01_1979-10-05_03:00:00.nc",
                    "original_shape": (1, 50, 1015, 1367),
                    "dtype": np.dtype("<f4"),
                    "coordinates": "XLONG XLAT XTIME",
                },
                "file": "wy1981/198101/wrf2d_d01_1981-01-30_19:00:00.nc",
                "file_path": "/tmp/input/wrf2d_d01_1981-01-30_19:00:00.nc",
            },
            {
                "variable": "T2",
                "target_chunks": (1, 100, 100),
                "target_encoding": {
                    "chunksizes": (1, 17, 339, 456),
                    "fletcher32": False,
                    "shuffle": True,
                    "preferred_chunks": {
                        "Time": 1,
                        "bottom_top": 17,
                        "south_north": 339,
                        "west_east": 456,
                    },
                    "zlib": True,
                    "complevel": np.int32(1),
                    "source": "/gdex/data/d559000/wy1980/197910/wrf3d_d01_1979-10-05_03:00:00.nc",
                    "original_shape": (1, 50, 1015, 1367),
                    "dtype": np.dtype("<f4"),
                    "coordinates": "XLONG XLAT XTIME",
                },
                "file": "wy1981/198101/wrf2d_d01_1981-01-31_19:00:00.nc",
                "file_path": "/tmp/input/wrf2d_d01_1981-01-31_19:00:00.nc",
            },
        ]
    )

    write_rechunk_plan_parquet(mock_df, str(parquet_path))
    loaded_df = read_rechunk_plan_parquet(str(parquet_path))

    assert parquet_path.exists()
    assert len(loaded_df) == len(mock_df)
    assert set(loaded_df.columns) == {
        "variable",
        "target_chunks",
        "target_encoding",
        "file",
        "file_path",
    }

    first_encoding = loaded_df.loc[0, "target_encoding"]
    assert isinstance(first_encoding, dict)
    assert first_encoding["dtype"] == "float32"
    assert first_encoding["zlib"] is True
    assert first_encoding["complevel"] == 1
    assert first_encoding["fletcher32"] is False
    assert first_encoding["shuffle"] is True
    assert first_encoding["preferred_chunks"] == {
        "Time": 1,
        "bottom_top": 17,
        "south_north": 339,
        "west_east": 456,
    }
    assert first_encoding["chunksizes"] == [1, 17, 339, 456]
    assert first_encoding["original_shape"] == [1, 50, 1015, 1367]
    assert first_encoding["coordinates"] == "XLONG XLAT XTIME"

    first_chunks = loaded_df.loc[0, "target_chunks"]
    assert isinstance(first_chunks, list)
    assert first_chunks == [1, 100, 100]
