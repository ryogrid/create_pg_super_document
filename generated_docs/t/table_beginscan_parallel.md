# table_beginscan_parallel

## Location
src/backend/access/table/tableam.c: 166 - 208

## Overview
Initiates a parallel table scan using a shared ParallelTableScanDesc structure, enabling coordinated scanning across multiple worker processes.

## Definition
```c
TableScanDesc table_beginscan_parallel(Relation relation, ParallelTableScanDesc pscan)
```

## Detailed Description
This function creates a TableScanDesc for parallel scanning by coordinating with other worker processes through a shared ParallelTableScanDesc structure. It handles snapshot restoration from serialized data or uses SnapshotAny for special cases. The function sets up appropriate scan flags for sequential scanning with strategy, synchronization, and page mode support, then delegates to the table access method's scan_begin function.

## Parameters / Member Variables
- `relation`: The Relation object representing the table to be scanned
- `pscan`: ParallelTableScanDesc structure containing shared scan state and coordination information

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid
  - [RestoreSnapshot](../R/RestoreSnapshot.md)  
  - RegisterSnapshot
  - SO_TYPE_SEQSCAN
  - SO_ALLOW_STRAT
  - SO_ALLOW_SYNC
  - SO_ALLOW_PAGEMODE
  - SO_TEMP_SNAPSHOT
  - SnapshotAny
- Called from (representative examples):
  - [_brin_parallel_scan_and_build](../b/_brin_parallel_scan_and_build.md)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md)
  - [ExecSeqScanInitializeDSM](../E/ExecSeqScanInitializeDSM.md)
  - [ExecSeqScanInitializeWorker](../E/ExecSeqScanInitializeWorker.md)

## Notes and Other Information
- The function validates that the relation ID matches the one stored in the parallel scan descriptor
- Handles two snapshot scenarios: serialized snapshots that need restoration, and SnapshotAny for special scanning cases
- Sets multiple scan optimization flags to enable efficient parallel scanning
- Integrates with PostgreSQL's table access method (tableam) interface for storage engine independence