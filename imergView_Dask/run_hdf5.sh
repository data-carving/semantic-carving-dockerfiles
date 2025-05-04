#!/bin/bash

ORIGINAL_LOG_FILE="/shared/original_results.txt"

echo HDF5 SELECTIONS CARVING > $ORIGINAL_LOG_FILE
{ time python3 pf_search_h5py.py ;} 2>> $ORIGINAL_LOG_FILE
mkdir -p /shared/hdf5_selections/POMD/output
find "/shared/POMD/output" -type f -name "dyamond_raw_pr_625_01mmhr.npy" -exec sh -c 'mv "$1" /shared/hdf5_selections/POMD/output/dyamond_raw_pr_625_01mmhr.npy.original' _ {} \;

EXECUTION_LOG_FILE="/shared/execution_results.txt"
data_folder="/shared/POMD/discover"

echo HDF5 SELECTIONS CARVING > $EXECUTION_LOG_FILE
{ time LD_PRELOAD="../hdf5-selections-carving/h5carve.so" CARVE=true python3 pf_search_h5py.py ;} 2>> $EXECUTION_LOG_FILE
./get_carving_results_hdf5.sh
mkdir -p /shared/hdf5_selections/POMD/discover/202001
mkdir -p /shared/hdf5_selections/POMD/discover/202002
find "$data_folder/202001" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/hdf5_selections/POMD/discover/202001/$(basename "$1")' _ {} \;
find "$data_folder/202002" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/hdf5_selections/POMD/discover/202002/$(basename "$1")' _ {} \;
find "/shared/POMD/output" -type f -name "dyamond_raw_pr_625_01mmhr.npy" -exec sh -c 'mv "$1" /shared/hdf5_selections/POMD/output/dyamond_raw_pr_625_01mmhr.npy.execution' _ {} \;

RE_EXECUTION_LOG_FILE="/shared/re-execution_results.txt"

echo HDF5 SELECTIONS CARVING > $RE_EXECUTION_LOG_FILE
{ time python3 pf_search_h5py_re-execution.py ;} 2>> $RE_EXECUTION_LOG_FILE
find "/shared/hdf5_selections/POMD/output" -type f -name "dyamond_raw_pr_625_01mmhr.npy" -exec sh -c 'mv "$1" /shared/hdf5_selections/POMD/output/dyamond_raw_pr_625_01mmhr.npy.reexecution' _ {} \;

rm -rf "/shared/hdf5_selections/POMD/discover"