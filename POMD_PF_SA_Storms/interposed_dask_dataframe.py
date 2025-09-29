# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from dask.dataframe import * # makes all functions and classes in dask.dataframe available in interposed_dask_dataframe
import time
import pandas
import h5py
import dask
import dask.dataframe as dd
import numpy as np

df_dict = {}

def initialize_global_dict(filename, df, npartitions):
	global df_dict
	# print(df)
	df_dict[filename] = {
		"global_df": None,
		"partition_access_mask": None,
		"column_access": [],
		"npartitions": None
	}

	# npartitions = kwargs['npartitions']
	df_dict[filename]['npartitions'] = npartitions
	df_dict[filename]['partition_access_mask'] = np.zeros(npartitions)

	df_dict[filename]['global_df'] = df


def initialize_partition_numbers(partition, partition_info=None):
	if partition_info is not None:
		# print(partition_info)
		# return partition.assign(access=0, partition_number=partition_info['number'])
		return partition.assign(partition_number=partition_info['number'])
	else:
		# return partition.assign(access=0, partition_number=0)
		return partition.assign(partition_number=0)

def toggle_access_mask(partition, partition_info=None):
	if partition_info is not None:
		if partition_info['number'] in partition_numbers:
			return partition.assign(access=1, partition_number=partition_info['number'])
		else:
			return partition.assign(access=0, partition_number=0)
	else:
		return partition.assign(access=0, partition_number=0)

original_from_pandas = from_pandas
def from_pandas(*args, **kwargs):
	global df_dict
	filename = kwargs['filename']
	npartitions = kwargs['npartitions']

	del kwargs['filename']

	df = original_from_pandas(*args, **kwargs)
	df = df.assign(filename=filename)
	df = df.assign(npartitions=npartitions)

	df = df.map_partitions(initialize_partition_numbers)

	if not df_dict:
		initialize_global_dict(filename, df, npartitions)

	if not filename in df_dict:
		initialize_global_dict(filename, df, npartitions)

	return df

original_read_hdf = read_hdf
def read_hdf(*args, **kwargs):
	df = original_read_hdf(*args, **kwargs)
	
	return df.map_partitions(initialize_partition_numbers)

original_compute = dd.DataFrame.compute
def interposed_compute(*args, **kwargs):
	return_val = original_compute(*args, **kwargs)
	
	if not return_val.empty: # Edge case for when a compute call gives empty dataframe, otherwise results in plethora of errors
		filename = original_pandas__getitem__(return_val, 'filename').iloc[0]
				
		accessed_partition_numbers = return_val['partition_number'].unique()

		df_dict[filename]['partition_access_mask'][accessed_partition_numbers] = 1

		return return_val
	
	return return_val
dd.DataFrame.compute = interposed_compute

original_pandas__getitem__ = pandas.DataFrame.__getitem__
def interposed_pandas__get__item(*args, **kwargs):
	global df_dict

	if not args[0].empty:
		filename = original_pandas__getitem__(args[0], 'filename').iloc[0]
		npartitions = original_pandas__getitem__(args[0], 'npartitions').iloc[0]

		if not df_dict:
			initialize_global_dict(filename, args[0], npartitions)

		if filename not in df_dict:
			initialize_global_dict(filename, args[0], npartitions)

		if args[1] not in df_dict[filename]['column_access']:
			df_dict[filename]['column_access'].append(args[1])

	return original_pandas__getitem__(*args, **kwargs)	
pandas.DataFrame.__getitem__ = interposed_pandas__get__item

def output_carved():
	dd.DataFrame.compute = original_compute
	pandas.DataFrame.__getitem__ = original_pandas__getitem__

	for filename in df_dict.keys():
		df = df_dict[filename]['global_df']
		accessed_mask = df_dict[filename]['partition_access_mask']
		partitions_accessed_union = np.flatnonzero(accessed_mask)

		cols_needed = [
		    col for col in df_dict[filename]['column_access']
		    if col not in ('filename', 'npartitions')
		]
		
		to_drop = list(set(df.columns) - set(cols_needed))
		
		df = df.drop(columns=to_drop) 
		
		df = df.where(df["partition_number"].isin(partitions_accessed_union), 0)

		h5f = h5py.File(filename + '.carved', 'r+')
		
		df = df.compute()
		
		for column in df_dict[filename]['column_access']:
			if column == 'filename' or column == 'partition_number' or column == 'npartitions':
				continue
			
			carved_data = df[column]
			
			dataset = h5f[column]
			
			dataset[...] = carved_data
