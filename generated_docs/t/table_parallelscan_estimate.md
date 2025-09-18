# table_parallelscan_estimate

## Location
src/backend/access/table/tableam.c: 131 - 145

## Overview
Estimates the shared memory space required for a parallel table scan, including both snapshot and table access method specific requirements.

## Definition


## Detailed Description
This function calculates the total shared memory space needed to coordinate a parallel table scan across multiple worker processes. The estimation includes:

1. **Snapshot Space**: If the snapshot is an MVCC snapshot, it calculates the space needed to serialize and share the snapshot across parallel workers using EstimateSnapshotSpace()
2. **SnapshotAny Handling**: For SnapshotAny (which doesn't require serialization), no snapshot space is added
3. **Table Access Method Space**: Delegates to the relation's table access method to estimate the space needed for parallel scan coordination specific to that storage engine

The function ensures that sufficient shared memory is allocated for parallel scan coordination before the parallel operation begins.

## Parameters / Member Variables
- : The relation to be scanned in parallel
- : The snapshot that will be used for the parallel scan

## Dependencies
- Functions called/Symbols referenced:
  - IsMVCCSnapshot (to check if snapshot needs serialization)
  - EstimateSnapshotSpace (to calculate space needed for snapshot serialization)
  - [add_size](../a/add_size.md) (to safely add sizes while checking for overflow)
  - SnapshotAny (global variable representing a special non-MVCC snapshot)

- Called from (representative examples):
  - [_brin_parallel_estimate_shared](../b/_brin_parallel_estimate_shared.md)
  - [_bt_parallel_estimate_shared](../b/_bt_parallel_estimate_shared.md)
  - [ExecSeqScanEstimate](../E/ExecSeqScanEstimate.md)
  - table_scan_getnextslot_tidrange

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework
- The size estimation is critical for proper shared memory allocation in parallel operations
- MVCC snapshots require serialization to ensure all parallel workers see the same consistent view of data
- SnapshotAny is special because it doesn't provide transaction isolation, so no snapshot coordination is needed
- Different table access methods may have varying requirements for parallel scan coordination (e.g., heap vs. columnar storage)
- The function uses add_size() for safe arithmetic to prevent integer overflow in size calculations
- Accurate estimation prevents shared memory allocation failures during parallel query execution