# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

#!/bin/bash

ORIGINAL_LOG_FILE="/shared/original_results.txt"

echo HDF5 > $ORIGINAL_LOG_FILE
{ time python3 featureDB_make_labels_h5py.py ;} 2>> $ORIGINAL_LOG_FILE
mkdir -p /shared/pickles/hdf5_selections_original
mv ./pickles/* /shared/pickles/hdf5_selections_original

EXECUTION_LOG_FILE="/shared/execution_results.txt"
data_folder="/shared/imerg2022"

echo HDF5 SELECTIONS CARVING > $EXECUTION_LOG_FILE
{ time LD_PRELOAD="./hdf5-selections-carving/h5carve.so" CARVE=true python3 featureDB_make_labels_h5py.py ;} 2>> $EXECUTION_LOG_FILE
./get_carving_results_hdf5.sh
mv ./pickles/* /shared/pickles/hdf5_selections_execution
mkdir -p /shared/hdf5_selections/imerg2022
find "$data_folder" -type f -name "*.HDF5.carved" -exec sh -c 'mv "$1" /shared/hdf5_selections/imerg2022/$(basename "$1")' _ {} \;

RE_EXECUTION_LOG_FILE="/shared/re-execution_results.txt"

echo HDF5 > $RE_EXECUTION_LOG_FILE
{ time python3 featureDB_make_labels_h5py_re-execution.py ;} 2>> $RE_EXECUTION_LOG_FILE
mkdir -p /shared/pickles/hdf5_selections_re-execution
mv ./pickles/* /shared/pickles/hdf5_selections_re-execution

rm -rf "/shared/hdf5_selections/"
