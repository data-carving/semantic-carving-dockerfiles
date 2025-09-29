# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from dask.array import * # makes all functions and classes in dask.dataframe available in interposed_dask_dataframe
import time
import pandas
import h5py
import dask
import dask.array as da
import numpy as np

darr_dict = {}

def initialize_global_dict(datapath, darr):
	global darr_dict
	print("Number of blocks are", darr.numblocks)
	darr_dict[datapath] = {
		"global_array": None,
		"block_access_mask": None
	}

	darr_dict[datapath]['block_access_mask'] = np.full(darr.numblocks, 0)
	darr_dict[datapath]['global_array'] = darr

def check_block(block, datapath, block_info=None):
	info = block_info[0]

	loc = info['chunk-location']

	if block.any():
		darr_dict[datapath]['block_access_mask'][loc] = 1

	return block

def return_block(block, access_mask, block_info=None):
	info = block_info[0]
	
	chunk_shape = block_info[None]['chunk-shape']

	loc = info['chunk-location']

	if (access_mask[loc]):
		return block
	else:
		return np.zeros_like(block)

original_from_array = from_array
def from_array(*args, **kwargs):
	global darr_dict

	darr = original_from_array(*args, **kwargs)
	print(kwargs['name'])
	
	if not darr_dict:
		initialize_global_dict(kwargs['name'], darr)

	if kwargs['name'] not in darr_dict:
		initialize_global_dict(kwargs['name'], darr)

	return darr

original_compute = da.core.Array.compute
def interposed_compute(*args, **kwargs):
	global_array = darr_dict[args[0].datapath]['global_array']
	global_blocks = global_array.blocks

	original_compute(args[0].map_blocks(check_block, dtype=args[0].dtype, datapath=args[0].datapath))

	return_val = original_compute(*args, **kwargs)

	return return_val
da.core.Array.compute = interposed_compute


def output_carved(delete=False):
	data_to_carve = list(darr_dict.keys())

	for datapath in data_to_carve:
		access_mask = darr_dict[datapath]['block_access_mask']

		darr = darr_dict[datapath]['global_array']

		carved_data = original_compute(darr.map_blocks(return_block, dtype=darr.dtype, access_mask=access_mask, chunks=darr.chunksize))

		filename = '/'.join(datapath.split('+')[:-1])
		dataset_name = datapath.split('+')[-1]

		h5f = h5py.File(filename + '.carved', 'r+')
		
		dataset = h5f[dataset_name]
			
		dataset[...] = carved_data

		if (delete):
			del darr_dict[datapath]
