#!/bin/bash

LOG_FILE="/shared/original_results.txt"

echo HDF5 SELECTIONS CARVING > $LOG_FILE
{ time python3 pf_search_h5py.py ;} 2>> $LOG_FILE
mkdir -p /shared/hdf5_selections/output
find "/shared/hdf5_selections/discover/output" -type f -name "dyamond_raw_pr_625_01mmhr.npy" -exec sh -c 'mv "$1" /shared/hdf5_selections/discover/output/dyamond_raw_pr_625_01mmhr.npy.original' _ {} \;

echo DASK CARVING >> $LOG_FILE
chunkshape="(1, 1800, 3600)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time python3 pf_search_dask_original.py "$chunkshape" ;} 2>> $LOG_FILE
mkdir -p /shared/dask_selections_1_1800_3600/output
find "/shared/dask_selections_1_1800_3600/discover/output" -type f -name "dyamond_raw_pr_625_01mmhr.npy" -exec sh -c 'mv "$1" /shared/dask_selections_1_1800_3600/discover/output/dyamond_raw_pr_625_01mmhr.npy.original' _ {} \;

chunkshape="(1, 450, 900)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time python3 pf_search_dask_original.py "$chunkshape" ;} 2>> $LOG_FILE
mkdir -p /shared/dask_selections_1_450_900/output
find "/shared/dask_selections_1_450_900/discover/output" -type f -name "dyamond_raw_pr_625_01mmhr.npy" -exec sh -c 'mv "$1" /shared/dask_selections_1_450_900/discover/output/dyamond_raw_pr_625_01mmhr.npy.original' _ {} \;

chunkshape="(1, 100, 225)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time python3 pf_search_dask_original.py "$chunkshape" ;} 2>> $LOG_FILE
mkdir -p /shared/dask_selections_1_100_225/output
find "/shared/dask_selections_1_100_225/discover/output" -type f -name "dyamond_raw_pr_625_01mmhr.npy" -exec sh -c 'mv "$1" /shared/dask_selections_1_100_225/discover/output/dyamond_raw_pr_625_01mmhr.npy.original' _ {} \;

chunkshape="(1, 25, 50)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time python3 pf_search_dask_original.py "$chunkshape" ;} 2>> $LOG_FILE
mkdir -p /shared/dask_selections_1_25_50/output
find "/shared/dask_selections_1_25_50/discover/output" -type f -name "dyamond_raw_pr_625_01mmhr.npy" -exec sh -c 'mv "$1" /shared/dask_selections_1_25_50/discover/output/dyamond_raw_pr_625_01mmhr.npy.original' _ {} \;

chunkshape="(1, 6, 12)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time python3 pf_search_dask_original.py "$chunkshape" ;} 2>> $LOG_FILE
mkdir -p /shared/dask_selections_1_6_12/output
find "/shared/dask_selections_1_6_12/discover/output" -type f -name "dyamond_raw_pr_625_01mmhr.npy" -exec sh -c 'mv "$1" /shared/dask_selections_1_6_12/discover/output/dyamond_raw_pr_625_01mmhr.npy.original' _ {} \;
