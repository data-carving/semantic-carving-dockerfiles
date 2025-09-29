# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

#!/usr/bin/env python3
"""
Check that corresponding pickle files in several folders contain identical objects.

Folders compared
----------------
dask_execution, dask_original, dask_re-execution, h5py, h5py_re-execution

Files compared
--------------
data.pickle, labels.pickle, largest_100.pickle, largest_20.pickle, timestamps.pickle
"""

import os
import pickle
import numpy as np
from itertools import combinations

FOLDERS = os.listdir('/shared/pickles')

FILES = [
    "labels.pickle",
    "largest_100.pickle",
    "largest_20.pickle",
]


def load_pickle(path):
    with open(path, "rb") as fp:
        return pickle.load(fp)


def objects_equal(a, b):
    """Robust equality that handles NumPy arrays as well as built-ins."""
    return np.array_equal(a, b)  # exact match


def compare_file_across_folders(fname):
    """Return a list of folder pairs whose objects differ for this file."""
    objs = {folder: load_pickle(os.path.join('/shared/pickles', folder, fname)) for folder in FOLDERS}
    mismatches = []
    for f1, f2 in combinations(FOLDERS, 2):
        if not objects_equal(objs[f1], objs[f2]):
            mismatches.append((f1, f2))
    return mismatches


def main():
    all_ok = True
    for fname in FILES:
        mismatches = compare_file_across_folders(fname)
        if not mismatches:
            print(f"{fname}: objects identical in all folders")
        else:
            all_ok = False
            print(f"{fname}: differences found between:")
            for f1, f2 in mismatches:
                print(f"{f1} != {f2}")
    if all_ok:
        print("\nAll files match across all folders")
    else:
        print("\nSome files differ — see details above.")


if __name__ == "__main__":
    main()
