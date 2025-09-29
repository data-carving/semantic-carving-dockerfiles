# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import os
import numpy as np
import glob

root_dirs = [
    "/shared/hdf5_selections/POMD/output",
	"/shared/dask_chunks_1_1800_3600/POMD/output",
    "/shared/dask_chunks_1_450_900/POMD/output",
    "/shared/dask_chunks_1_100_225/POMD/output"
    "/shared/dask_chunks_1_25_50/POMD/output"
    # "/shared/dask_chunks_1_6_12/POMD/output"
]

files = []
for directory in root_dirs:
	pattern = os.path.join(directory,  "*.npy.*")
	files += glob.glob(pattern)

data = []
for file in files:
	print(file)
	data.append(np.load(file))

ref = data[0]

if all(np.array_equal(arr, ref) for arr in data[1:]):
	print("All .npy files generated are equal.")
else:
	print(".npy files generated are not equal.")
