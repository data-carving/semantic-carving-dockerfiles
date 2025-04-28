#!/bin/bash

LOG_FILE="/shared/execution_results.txt"
data_folder="/shared/POMD/discover"

echo HDF5 SELECTIONS CARVING > $LOG_FILE
{ time LD_PRELOAD="../hdf5-selections-carving/h5carve.so" CARVE=true python3 pf_search_h5py.py ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/hdf5_selections/discover/202001
mkdir -p /shared/hdf5_selections/discover/202002
find "$data_folder/202001" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/hdf5_selections/discover/202001/$(basename "$1")' _ {} \;
find "$data_folder/202002" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/hdf5_selections/discover/202002/$(basename "$1")' _ {} \;

echo DASK CARVING >> $LOG_FILE
chunkshape="(1, 1800, 3600)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time LD_PRELOAD="../hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 pf_search_dask_execution.py "$chunkshape" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_selections_1_1800_3600/discover/202001
mkdir -p /shared/dask_selections_1_1800_3600/discover/202002
find "$data_folder/202001" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_1800_3600/discover/202001/$(basename "$1")' _ {} \;
find "$data_folder/202002" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_1800_3600/discover/202002/$(basename "$1")' _ {} \;

chunkshape="(1, 450, 900)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time LD_PRELOAD="../hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 pf_search_dask_execution.py "$chunkshape" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_selections_1_450_900/discover/202001
mkdir -p /shared/dask_selections_1_450_900/discover/202002
find "$data_folder/202001" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_450_900/discover/202001/$(basename "$1")' _ {} \;
find "$data_folder/202002" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_450_900/discover/202002/$(basename "$1")' _ {} \;

chunkshape="(1, 100, 225)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time LD_PRELOAD="../hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 pf_search_dask_execution.py "$chunkshape" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_selections_1_100_225/discover/202001
mkdir -p /shared/dask_selections_1_100_225/discover/202002
find "$data_folder/202001" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_100_225/discover/202001/$(basename "$1")' _ {} \;
find "$data_folder/202002" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_100_225/discover/202002/$(basename "$1")' _ {} \;

chunkshape="(1, 25, 50)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time LD_PRELOAD="../hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 pf_search_dask_execution.py "$chunkshape" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_selections_1_25_50/discover/202001
mkdir -p /shared/dask_selections_1_25_50/discover/202002
find "$data_folder/202001" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_25_50/discover/202001/$(basename "$1")' _ {} \;
find "$data_folder/202002" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_25_50/discover/202002/$(basename "$1")' _ {} \;

chunkshape="(1, 6, 12)"
echo CHUNKSHAPE "$chunkshape" >> $LOG_FILE
{ time LD_PRELOAD="../hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 pf_search_dask_execution.py "$chunkshape" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_selections_1_6_12/discover/202001
mkdir -p /shared/dask_selections_1_6_12/discover/202002
find "$data_folder/202001" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_6_12/discover/202001/$(basename "$1")' _ {} \;
find "$data_folder/202002" -type f -name "*.nc4.carved" -exec sh -c 'mv "$1" /shared/dask_selections_1_6_12/discover/202002/$(basename "$1")' _ {} \;

