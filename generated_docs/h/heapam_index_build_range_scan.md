# heapam_index_build_range_scan

## Location
[src/backend/access/heap/heapam_handler.c:1173-1747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L1173-L1747)

## Overview
Performs a range scan of a heap relation to build index entries, handling transaction visibility, HOT chains, and parallel scanning during index creation.

## Definition
```c
static double heapam_index_build_range_scan(Relation heapRelation,
                                          Relation indexRelation,
                                          IndexInfo *indexInfo,
                                          bool allow_sync,
                                          bool anyvisible,
                                          bool progress,
                                          BlockNumber start_blockno,
                                          BlockNumber numblocks,
                                          IndexBuildCallback callback,
                                          void *callback_state,
                                          TableScanDesc scan)
```

## Detailed Description
This function is the core implementation for heap table scanning during index builds. It handles complex tuple visibility checking using different snapshot strategies based on the build type (serial vs concurrent, bootstrap vs normal). The function manages HOT (Heap-Only-Tuples) chains by ensuring index entries point to root tuples, preserving chain structure. It supports both serial and parallel index builds, with comprehensive progress reporting and proper handling of transaction isolation levels.

The function performs detailed visibility checks for each tuple, deciding whether to index it based on transaction state (LIVE, DEAD, RECENTLY_DEAD, INSERT_IN_PROGRESS, DELETE_IN_PROGRESS). For concurrent transactions, it may wait for completion to ensure proper uniqueness checking. It extracts index attribute values from tuples and calls the index access method callback to process each qualifying tuple.

## Parameters / Member Variables
- `heapRelation`: The heap table being scanned for index building
- `indexRelation`: The index being constructed
- `indexInfo`: Metadata about the index including uniqueness constraints and predicates
- `allow_sync`: Whether synchronized scanning is permitted for performance
- `anyvisible`: Special mode that considers all visible tuples regardless of transaction state
- `progress`: Whether to report scan progress for monitoring
- `start_blockno`: Starting block number for range scanning
- `numblocks`: Number of blocks to scan (InvalidBlockNumber for all)
- `callback`: Index AM callback function to process each tuple
- `callback_state`: State data passed to the callback function
- `scan`: Optional pre-existing table scan descriptor for parallel builds

## Dependencies
- Functions called/Symbols referenced:
  - [heap_getnext](heap_getnext.md)
  - [heapam_scan_get_blocks_done](heapam_scan_get_blocks_done.md)
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - [heap_get_root_tuples](heap_get_root_tuples.md)
  - [FormIndexDatum](../F/FormIndexDatum.md)
  - ExecQual
  - [table_beginscan_strat](../t/table_beginscan_strat.md)
  - [GetOldestNonRemovableTransactionId](../G/GetOldestNonRemovableTransactionId.md)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)

## Notes and Other Information
This function implements sophisticated visibility logic to handle various tuple states during index building. It must balance performance (via synchronized scanning) with correctness (proper visibility checking). The HOT chain handling ensures that index entries maintain proper relationships with heap tuples even after updates. The function supports both snapshot-based visibility (for concurrent builds) and custom visibility logic (for regular builds) to maintain MVCC semantics throughout the index creation process.