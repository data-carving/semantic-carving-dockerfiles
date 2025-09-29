# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

#!/bin/bash

in="$1"                       # e.g. "(1, 450, 900)"
clean=${in//[[:space:]\(\)]/} # → "1,450,900"
underscore=${clean//,/_}      # → "1_450_900"   (destination root)
xsep=${clean//,/x}            # → "1x450x900"   (h5repack syntax)

mkdir -p "/shared/dask_chunks_$underscore"/POMD/discover/202001
mkdir -p "/shared/dask_chunks_$underscore"/POMD/discover/202002

echo "Rechunking files with chunk shape $xsep …"
find /shared/POMD/discover/20200[12] -type f -name '*.nc4' -print0 |
  while IFS= read -r -d '' f; do
      out="/shared/dask_chunks_$underscore${f#/shared}"
      echo "  $f → $out"
      h5repack -l /PRECTOT:CHUNK=$xsep "$f" "$out"
  done

echo "All done. Rechunked files are under /shared/dask_chunks_$underscore/."
