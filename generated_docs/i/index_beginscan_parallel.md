# index_beginscan_parallel

## Location
src/backend/access/index/indexam.c: 541 - 573

## Overview
The `index_beginscan_parallel` function initializes and joins a parallel index scan, allowing a worker process to participate in a coordinated parallel index scanning operation.

## Definition
```c
IndexScanDesc index_beginscan_parallel(Relation heaprel, Relation indexrel, 
                                     int nkeys, int norderbys, 
                                     ParallelIndexScanDesc pscan)
```

## Detailed Description
This function enables a worker process to join an existing parallel index scan that was set up by a leader process. It performs several critical steps:

1. Validates that the heap relation ID matches the parallel scan descriptor
2. Restores the snapshot from the parallel scan descriptor and registers it
3. Calls `index_beginscan_internal` with parallel scan parameters to initialize the scan
4. Sets up additional scan descriptor fields including heap relation and snapshot references
5. Initializes heap tuple fetching capabilities for retrieving actual table data

The function leverages the already-processed `index_beginscan_internal` for the core scan initialization, but adds parallel-specific setup and snapshot management.

## Parameters / Member Variables
- `heaprel`: The heap relation being scanned
- `indexrel`: The index relation used for scanning
- `nkeys`: Number of scan keys (search conditions)
- `norderbys`: Number of order-by expressions
- `pscan`: Parallel index scan descriptor containing shared state and snapshot information

## Dependencies
- Functions called/Symbols referenced:
  - `RestoreSnapshot` (restores snapshot from serialized data)
  - `RegisterSnapshot` (registers snapshot for transaction management)
  - `index_beginscan_internal` (core scan initialization)
  - `table_index_fetch_begin` (initializes heap tuple fetching)
  - `ParallelIndexScanDesc` (parallel scan descriptor type)
  - `IndexScanDesc` (scan descriptor type)
- Called from (representative examples):
  - `ExecIndexOnlyScanInitializeWorker` (src/backend/executor/nodeIndexonlyscan.c:784)
  - `ExecIndexScanInitializeWorker` (src/backend/executor/nodeIndexscan.c:1717)

## Notes and Other Information
- The caller must hold appropriate locks on both heap and index relations before calling this function
- The function assumes the parallel scan descriptor contains valid snapshot data
- This is part of PostgreSQL's parallel query execution infrastructure using dynamic shared memory
- The restored snapshot ensures all parallel workers see a consistent view of the data
- Location: src/backend/access/index/indexam.c:541-573