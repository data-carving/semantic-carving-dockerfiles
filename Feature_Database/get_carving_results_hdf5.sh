# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

#!/bin/bash

LOG_FILE="/shared/execution_results.txt"
data_folder="/shared/imerg2022"

echo "Original Files" >> $LOG_FILE
find $data_folder -type f -name "*.HDF5" -exec du -sh {} \; >> $LOG_FILE
echo "Carved Files" >> $LOG_FILE 
find $data_folder -type f -name "*.HDF5.carved" -exec du -sh {} \; >> $LOG_FILE

SUM_ORIGINAL=$(find $data_folder -type f -name "*.HDF5" -exec du -b {} \; | awk '{sum += $1} END {printf "%.0f", sum}')
SUM_CARVED=$(find $data_folder -type f -name "*.HDF5.carved" -exec du -b {} \; | awk '{sum += $1} END {printf "%.0f", sum}')
REDUCTION=$(expr $SUM_ORIGINAL - $SUM_CARVED)
REDUCTION_RATIO=$(echo "scale=4; ($REDUCTION / $SUM_ORIGINAL)" | bc)
REDUCTION_PERCENTAGE=$(echo "scale=2; ($REDUCTION_RATIO * 100)" | bc | awk '{printf "%.2f", $1}')

echo "Percentage Reduction" >> $LOG_FILE 
echo -e "$REDUCTION_PERCENTAGE%\n" >> $LOG_FILE
