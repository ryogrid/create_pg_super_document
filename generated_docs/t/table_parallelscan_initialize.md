# table_parallelscan_initialize

## Location
src/backend/access/table/tableam.c: 146 - 165

## Overview
Initializes a parallel table scan descriptor by setting up snapshot serialization and delegating table access method specific initialization.

## Definition


## Detailed Description
This function initializes the shared memory structures needed for coordinating a parallel table scan across multiple worker processes. The initialization process involves:

1. **Table Access Method Initialization**: Delegates to the relation's table access method to initialize storage-engine-specific parallel scan structures, which returns an offset for snapshot storage
2. **Snapshot Handling**: 
   - For MVCC snapshots: Serializes the snapshot into the shared memory area using SerializeSnapshot() and marks phs_snapshot_any as false
   - For SnapshotAny: Sets phs_snapshot_any to true without serialization since SnapshotAny doesn't require coordination
3. **Offset Management**: Stores the snapshot offset in the parallel scan descriptor for later access by worker processes

This function must be called in the leader process before parallel workers are launched to ensure proper shared memory setup.

## Parameters / Member Variables
- : The relation that will be scanned in parallel
- : The ParallelTableScanDesc structure in shared memory that coordinates the parallel scan
- : The snapshot to be used by all parallel workers

## Dependencies
- Functions called/Symbols referenced:
  - IsMVCCSnapshot (to determine if snapshot needs serialization)
  - SerializeSnapshot (to serialize MVCC snapshot into shared memory)
  - SnapshotAny (global variable representing special non-MVCC snapshot)
  - ParallelTableScanDesc (structure type for parallel scan coordination)

- Called from (representative examples):
  - _brin_begin_parallel
  - _bt_begin_parallel  
  - ExecSeqScanInitializeDSM
  - table_scan_getnextslot_tidrange

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework
- The function must be called by the parallel leader process before workers are spawned
- MVCC snapshot serialization ensures all parallel workers see exactly the same consistent view of the database
- SnapshotAny doesn't require serialization because it provides no transaction isolation guarantees
- The snapshot offset returned by the table access method allows the snapshot to be stored after the AM-specific parallel scan data
- Different storage engines may have different requirements for parallel scan coordination (heap, btree, etc.)
- The initialized ParallelTableScanDesc is used by worker processes to coordinate their scanning activities
- Proper initialization is critical for parallel query correctness and performance