# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

#!/bin/bash

ORIGINAL_LOG_FILE="/shared/original_results.txt"
chunkshape=$1
clean=${chunkshape//[[:space:]\(\)]/}
underscore=${clean//,/_}

./rechunk.sh "$chunkshape"

echo DASK CARVING >> $ORIGINAL_LOG_FILE
echo CHUNKSHAPE "$chunkshape" >> $ORIGINAL_LOG_FILE
mkdir -p "/shared/dask_chunks_$underscore/POMD/output"
{ time python3 pf_search_dask_original.py "$chunkshape" ;} 2>> $ORIGINAL_LOG_FILE
mv "/shared/dask_chunks_${underscore}/POMD/output/dyamond_raw_pr_625_01mmhr.npy" "/shared/dask_chunks_${underscore}/POMD/output/dyamond_raw_pr_625_01mmhr.npy.original"

EXECUTION_LOG_FILE="/shared/execution_results.txt"
data_folder="/shared/dask_chunks_${underscore}/POMD/discover"

echo DASK CARVING >> $EXECUTION_LOG_FILE
echo CHUNKSHAPE "$chunkshape" >> $EXECUTION_LOG_FILE
{ time LD_PRELOAD="../hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 pf_search_dask_execution.py "$chunkshape" ;} 2>> $EXECUTION_LOG_FILE
./get_carving_results_dask.sh "$chunkshape"
mv "/shared/dask_chunks_${underscore}/POMD/output/dyamond_raw_pr_625_01mmhr.npy" "/shared/dask_chunks_${underscore}/POMD/output/dyamond_raw_pr_625_01mmhr.npy.execution"

RE_EXECUTION_LOG_FILE="/shared/re-execution_results.txt"

echo DASK CARVING >> $RE_EXECUTION_LOG_FILE
echo CHUNKSHAPE "$chunkshape" >> $RE_EXECUTION_LOG_FILE
{ time python3 pf_search_dask_re-execution.py "$chunkshape" ;} 2>> $RE_EXECUTION_LOG_FILE
mv "/shared/dask_chunks_${underscore}/POMD/output/dyamond_raw_pr_625_01mmhr.npy" "/shared/dask_chunks_${underscore}/POMD/output/dyamond_raw_pr_625_01mmhr.npy.reexecution"

rm -rf "/shared/dask_chunks_${underscore}/POMD/discover"
