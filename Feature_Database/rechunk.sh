#!/bin/bash

in="$1"                       # e.g. "(1, 450, 900)"
clean=${in//[[:space:]\(\)]/} # → "1,450,900"
underscore=${clean//,/_}      # → "1_450_900"   (destination root)
xsep=${clean//,/x}            # → "1x450x900"   (h5repack syntax)

mkdir -p "/shared/dask_chunks_$underscore"/imerg2022
mkdir -p "/shared/dask_chunks_$underscore"/imerg2022

echo "Rechunking files with chunk shape $xsep …"
find /shared/imerg2022 -type f -name '*.HDF5' -print0 |
  while IFS= read -r -d '' f; do
      out="/shared/dask_chunks_$underscore${f#/shared}"
      echo "  $f → $out"
      h5repack -l /Grid/precipitationCal:CHUNK=$xsep "$f" "$out"
  done

echo "All done. Rechunked files are under /shared/dask_chunks_$underscore/."