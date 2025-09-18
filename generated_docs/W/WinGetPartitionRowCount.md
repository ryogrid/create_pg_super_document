# WinGetPartitionRowCount

## Location
src/backend/executor/nodeWindowAgg.c: 3200 - 3217

## Overview
Returns the total number of rows in the current partition, forcing complete partition spooling into the tuplestore if not already done.

## Definition


## Detailed Description
This function provides the total row count for the current partition being processed by a window function. It implements a lazy loading strategy where the entire partition is spooled into the tuplestore on the first call, which can be expensive for large partitions. However, subsequent calls within the same partition are very efficient as they simply return the cached count. The function forces complete partition materialization by calling spool_tuples with -1 (meaning "spool all remaining tuples"), ensuring that all rows in the partition are available for analysis. This is essential for window functions that need to know the partition size for calculations like PERCENT_RANK, CUME_DIST, and NTILE.

## Parameters / Member Variables
- : WindowObject containing the window state and spooled row information

## Dependencies
- Functions called/Symbols referenced:
  - WindowObjectIsValid
  - spool_tuples
- Called from (representative examples):
  - window_percent_rank
  - window_cume_dist
  - window_ntile

## Notes and Other Information
- Returns int64 representing the total number of rows in the current partition
- First call within a partition is expensive as it forces complete spooling
- Subsequent calls within the same partition are cheap (just return cached value)
- Essential for statistical window functions that need partition size
- Forces materialization of the entire partition in memory via tuplestore
- Validates WindowObject before proceeding with spooling operation
- Uses spool_tuples(-1) to ensure all partition rows are loaded