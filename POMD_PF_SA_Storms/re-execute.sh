#!/bin/bash

LOG_FILE="/shared/re-execution_results.txt"

echo HDF5 > $LOG_FILE
{ time python3 POMD-PF.AIST.SA.Storms_h5py_selections_re-execution.py ;} 2>> $LOG_FILE

echo -e '\nDASK' >> $LOG_FILE
npartitions="1"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_re-execution.py "$npartitions" ;} 2>> $LOG_FILE

echo -e '\nDASK' >> $LOG_FILE
npartitions="10"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_re-execution.py "$npartitions" ;} 2>> $LOG_FILE

echo -e '\nDASK' >> $LOG_FILE
npartitions="100"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_re-execution.py "$npartitions" ;} 2>> $LOG_FILE

echo -e '\nDASK' >> $LOG_FILE
npartitions="1000"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_re-execution.py "$npartitions" ;} 2>> $LOG_FILE

echo -e '\nDASK' >> $LOG_FILE
npartitions="10000"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_re-execution.py "$npartitions" ;} 2>> $LOG_FILE

echo -e '\nDASK' >> $LOG_FILE
npartitions="100000"
echo NPARTITIONS "$npartitions" >> $LOG_FILE
{ time python3 POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_re-execution.py "$npartitions" ;} 2>> $LOG_FILE