# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

#!/bin/bash

./run_hdf5.sh

./run_dask.sh "(1, 1800, 3600)"
./run_dask.sh "(1, 450, 900)"
./run_dask.sh "(1, 100, 225)"
./run_dask.sh "(1, 25, 50)"

python3 check_npy_files.py
