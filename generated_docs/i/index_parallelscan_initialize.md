# index_parallelscan_initialize

## Location
[src/backend/access/index/indexam.c:490-522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L490-L522)

## Overview
The index_parallelscan_initialize function sets up and initializes a parallel index scan descriptor in shared memory, preparing it for use by multiple worker processes in parallel query execution.

## Definition
```c
void index_parallelscan_initialize(Relation heapRelation, Relation indexRelation, Snapshot snapshot, ParallelIndexScanDesc target)
```

## Detailed Description
index_parallelscan_initialize initializes both the common ParallelIndexScanDesc structure and any access method-specific data needed for parallel scanning. This function is called once by the leader process during parallel query setup. It serializes the snapshot into shared memory, records the relation IDs, calculates memory offsets, and delegates AM-specific initialization to the access method's aminitparallelscan routine if available.

The function prepares the shared memory structure that will be accessed by multiple worker processes, ensuring that all necessary information for parallel scanning is properly stored and aligned in the shared memory segment.

## Parameters / Member Variables
- `heapRelation`: Relation - The heap relation being scanned (for relation ID storage)
- `indexRelation`: Relation - The index relation to be used for parallel scanning
- `snapshot`: Snapshot - The snapshot to be serialized and shared among workers
- `target`: ParallelIndexScanDesc - The shared memory descriptor to initialize

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_CHECKS (validation macro for relations)
  - [add_size](../a/add_size.md) (safe size addition utility)
  - [ParallelIndexScanDescData](../P/ParallelIndexScanDescData.md) (parallel scan descriptor structure)
  - EstimateSnapshotSpace (estimates snapshot serialization space)
  - MAXALIGN (memory alignment macro)
  - RelationGetRelid (gets relation OID)
  - [SerializeSnapshot](../S/SerializeSnapshot.md) (serializes snapshot to shared memory)
  - OffsetToPointer (converts offset to memory pointer)
  - aminitparallelscan (AM-specific parallel scan initialization)
- Called from (representative examples):
  - [ExecIndexScanInitializeDSM](../E/ExecIndexScanInitializeDSM.md)
  - [ExecIndexOnlyScanInitializeDSM](../E/ExecIndexOnlyScanInitializeDSM.md)

## Notes and Other Information
- Called only once by the leader process during parallel query setup
- Worker processes attach to the scan via index_beginscan_parallel, not this function
- Handles access methods that don't provide aminitparallelscan by treating it as a no-op
- Stores both heap and index relation IDs for worker process reference
- Serializes the snapshot to ensure consistent visibility across all parallel workers
- Memory layout includes the base descriptor, serialized snapshot, and optional AM-specific data
- Located in src/backend/access/index/indexam.c:490-522
- Essential component of PostgreSQL's parallel query execution infrastructure