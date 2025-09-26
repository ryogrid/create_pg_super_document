# heap_beginscan

## Location
[src/backend/access/heap/heapam.c:1082-1195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1082-L1195)

## Overview
Initializes and returns a new heap table scan descriptor for sequential scanning of a relation with support for parallel scanning, snapshots, scan keys, and various optimization flags.

## Definition

```c
TableScanDesc
heap_beginscan(Relation relation, Snapshot snapshot,
			   int nkeys, ScanKey key,
			   ParallelTableScanDesc parallel_scan,
			   uint32 flags)
```
## Detailed Description
This function serves as the entry point for heap table scanning operations. It allocates and initializes a HeapScanDesc structure, increments the relation reference count for safety, and sets up various scan parameters including snapshot, scan keys, and flags. The function handles special considerations for different scan types: disables page-at-a-time mode for non-MVCC snapshots, acquires predicate locks for serializable transactions on sequential and sample scans, allocates parallel worker data for parallel scans, and sets up read streams for sequential and TID range scans with appropriate callback functions for parallel vs serial execution.

## Parameters / Member Variables
- : Relation - The heap relation to scan
- : Snapshot - The snapshot to use for visibility checking (can be NULL for certain scan types)
- : int - Number of scan key conditions for filtering
- : ScanKey - Array of scan key conditions (can be NULL if nkeys is 0)
- : ParallelTableScanDesc - Parallel scan descriptor for coordinated parallel scanning (NULL for non-parallel)
- : uint32 - Scan behavior flags (SO_TYPE_SEQSCAN, SO_ALLOW_PAGEMODE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [RelationIncrementReferenceCount](../R/RelationIncrementReferenceCount.md)
  - [palloc](../p/palloc.md)
  - IsMVCCSnapshot
  - [PredicateLockRelation](../P/PredicateLockRelation.md)
  - RelationGetRelid
  - [initscan](../i/initscan.md)
  - [read_stream_begin_relation](../r/read_stream_begin_relation.md)
  - [heap_scan_stream_read_next_parallel](heap_scan_stream_read_next_parallel.md)
  - [heap_scan_stream_read_next_serial](heap_scan_stream_read_next_serial.md)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)
  - HeapScanIsValid

## Notes and Other Information
- Increments relation reference count to prevent relcache entry removal during scan
- Page-at-a-time mode is disabled for non-MVCC snapshots due to visibility consistency requirements
- Predicate locking is applied to entire relations for sequential and sample scans in serializable transactions
- Parallel scan support includes allocation of ParallelBlockTableScanWorkerData for coordination
- Read streams are set up for sequential and TID range scans with different callbacks for parallel vs serial execution
- The rs_strategy field is set later in initscan() rather than during initial allocation
- Memory allocation for scan keys is done here rather than in initscan() to avoid reallocation during rescan operations