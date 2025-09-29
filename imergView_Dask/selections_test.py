# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import h5py
import numpy as np
import numpy.ma as ma

pr_floor = 0.1 / 3600.0
path="/shared/POMD/discover/202001/"
mfile = [
        "DYAMONDv2_PE3600x1800-DE.tavg_30mn.prectot.20200115_0000z.nc4", 
        "DYAMONDv2_PE3600x1800-DE.tavg_30mn.prectot.20200115_0030z.nc4",
        "DYAMONDv2_PE3600x1800-DE.tavg_30mn.prectot.20200115_0100z.nc4",
        "DYAMONDv2_PE3600x1800-DE.tavg_30mn.prectot.20200115_0130z.nc4",
        "DYAMONDv2_PE3600x1800-DE.tavg_30mn.prectot.20200115_0200z.nc4"
    ]

for file in mfile:
	ds = h5py.File(path+file, 'r')["PRECTOT"]

	##
	# Read PR Field
	#   (1, 1800, 3600) numpy.ma.core.MaskedArray numpy.float32
	_pr = ds[()]
	#   (1800, 3600)

	point_selection_mask = (_pr >= pr_floor)
	ds[point_selection_mask]

	_pr = _pr.squeeze()

	##
	# Remove mask
	_pr = ma.filled(_pr, 0)

	#ds.close()

	##
	# Mask with PR < 0.1 mm/hr
	_pr = np.where(_pr >= pr_floor, 1, 0)

	# pr_mask = np.nonzero(_pr >= pr_floor)
	pr_mask = np.nonzero(_pr)
