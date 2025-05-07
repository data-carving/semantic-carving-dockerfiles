#!/bin/bash

ORIGINAL_LOG_FILE="/shared/original_results.txt"
chunkshape=$1
clean=${chunkshape//[[:space:]\(\)]/}
underscore=${clean//,/_}

./rechunk.sh "$chunkshape"

echo -e "\nDASK" >> $ORIGINAL_LOG_FILE
echo CHUNKSHAPE "$chunkshape" >> $ORIGINAL_LOG_FILE
{ time python3 featureDB_make_labels_dask_original.py "$chunkshape" ;} 2>> $ORIGINAL_LOG_FILE
mkdir -p "/shared/pickles/dask_chunks_${underscore}_original"
mv ./pickles/* "/shared/pickles/dask_chunks_${underscore}_original"

EXECUTION_LOG_FILE="/shared/execution_results.txt"

echo -e "\nDASK CARVING" >> $EXECUTION_LOG_FILE
echo CHUNKSHAPE "$chunkshape" >> $EXECUTION_LOG_FILE
{ time LD_PRELOAD="./hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 featureDB_make_labels_dask_execution.py "$chunkshape" ;} 2>> $EXECUTION_LOG_FILE
./get_carving_results_dask.sh "$chunkshape"
mkdir -p "/shared/dask_chunks_${underscore}/imerg2022"
mkdir -p "/shared/pickles/dask_chunks_${underscore}_execution"
mv ./pickles/* "/shared/pickles/dask_chunks_${underscore}_execution"

RE_EXECUTION_LOG_FILE="/shared/re-execution_results.txt"

echo -e "\nDASK" >> $RE_EXECUTION_LOG_FILE
echo CHUNKSHAPE "$chunkshape" >> $RE_EXECUTION_LOG_FILE
{ time python3 featureDB_make_labels_dask_re-execution.py "$chunkshape" ;} 2>> $RE_EXECUTION_LOG_FILE
mkdir -p "/shared/pickles/dask_chunks_${underscore}_re-execution"
mv ./pickles/* "/shared/pickles/dask_chunks_${underscore}_re-execution"

rm -rf "/shared/dask_chunks_${underscore}/"