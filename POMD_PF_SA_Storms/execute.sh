#!/bin/bash

LOG_FILE="/shared/execution_results.txt"
data_folder="/shared/POMD/"

echo HDF5 SELECTIONS CARVING > $LOG_FILE
{ time LD_PRELOAD="./hdf5-selections-carving/h5carve.so" CARVE=true python3 POMD-PF.AIST.SA.Storms_h5py_selections.py ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/hdf5_selections/POMD/IMERG-PF/level2
mkdir -p /shared/hdf5_selections/POMD/GEOS5-PF/level2
find $data_folder"/IMERG-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/hdf5_selections/POMD/IMERG-PF/level2/$(basename "$1")' _ {} \;
find $data_folder"/GEOS5-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/hdf5_selections/POMD/GEOS5-PF/level2/$(basename "$1")' _ {} \;

echo DASK CARVING >> $LOG_FILE
npartitions="1"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time LD_PRELOAD="./hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe.py "$npartitions" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_partitions_1/POMD/IMERG-PF/level2
mkdir -p /shared/dask_partitions_1/POMD/GEOS5-PF/level2
find $data_folder"/IMERG-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_1/POMD/IMERG-PF/level2/$(basename "$1")' _ {} \;
find $data_folder"/GEOS5-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_1/POMD/GEOS5-PF/level2/$(basename "$1")' _ {} \;

echo DASK CARVING >> $LOG_FILE
npartitions="10"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time LD_PRELOAD="./hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe.py "$npartitions" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_partitions_10/POMD/IMERG-PF/level2
mkdir -p /shared/dask_partitions_10/POMD/GEOS5-PF/level2
find $data_folder"/IMERG-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_10/POMD/IMERG-PF/level2/$(basename "$1")' _ {} \;
find $data_folder"/GEOS5-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_10/POMD/GEOS5-PF/level2/$(basename "$1")' _ {} \;

echo DASK CARVING >> $LOG_FILE
npartitions="100"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time LD_PRELOAD="./hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe.py "$npartitions" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_partitions_100/POMD/IMERG-PF/level2
mkdir -p /shared/dask_partitions_100/POMD/GEOS5-PF/level2
find $data_folder"/IMERG-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_100/POMD/IMERG-PF/level2/$(basename "$1")' _ {} \;
find $data_folder"/GEOS5-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_100/POMD/GEOS5-PF/level2/$(basename "$1")' _ {} \;

echo DASK CARVING >> $LOG_FILE
npartitions="1000"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time LD_PRELOAD="./hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe.py "$npartitions" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_partitions_1000/POMD/IMERG-PF/level2
mkdir -p /shared/dask_partitions_1000/POMD/GEOS5-PF/level2
find $data_folder"/IMERG-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_1000/POMD/IMERG-PF/level2/$(basename "$1")' _ {} \;
find $data_folder"/GEOS5-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_1000/POMD/GEOS5-PF/level2/$(basename "$1")' _ {} \;

echo DASK CARVING >> $LOG_FILE
npartitions="10000"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time LD_PRELOAD="./hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe.py "$npartitions" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_partitions_10000/POMD/IMERG-PF/level2
mkdir -p /shared/dask_partitions_10000/POMD/GEOS5-PF/level2
find $data_folder"/IMERG-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_10000/POMD/IMERG-PF/level2/$(basename "$1")' _ {} \;
find $data_folder"/GEOS5-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_10000/POMD/GEOS5-PF/level2/$(basename "$1")' _ {} \;

echo DASK CARVING >> $LOG_FILE
npartitions="100000"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time LD_PRELOAD="./hdf5-selections-carving/h5carve.so" FILE_CREATE=true python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe.py "$npartitions" ;} 2>> $LOG_FILE
./get_carving_results.sh
#find $data_folder -type f -name "*.nc4.carved" -exec rm {} \;
mkdir -p /shared/dask_partitions_100000/POMD/IMERG-PF/level2
mkdir -p /shared/dask_partitions_100000/POMD/GEOS5-PF/level2
find $data_folder"/IMERG-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_100000/POMD/IMERG-PF/level2/$(basename "$1")' _ {} \;
find $data_folder"/GEOS5-PF" -type f -name "*.h5.carved" -exec sh -c 'mv "$1" /shared/dask_partitions_100000/POMD/GEOS5-PF/level2/$(basename "$1")' _ {} \;

./reproduce_results.sh