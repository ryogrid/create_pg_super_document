# ExecHashSkewTableInsert

## Location
[src/backend/executor/nodeHash.c:2581-2626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2581-L2626)

## Overview
Inserts a tuple into a skew hashtable bucket, managing memory allocation and space limits while maintaining the skew bucket's linked list structure.

## Definition

```c
static void
ExecHashSkewTableInsert(HashJoinTable hashtable,
						TupleTableSlot *slot,
						uint32 hashvalue,
						int bucketNumber)
```
## Detailed Description
ExecHashSkewTableInsert handles the insertion of tuples into the skew optimization hashtable during hash join processing. This function is specifically designed for tuples whose hash values correspond to most common values (MCVs) that have dedicated skew buckets.

The function creates a HashJoinTuple structure by extracting a MinimalTuple from the input slot and wrapping it with hash join metadata. The tuple is then inserted at the front of the linked list in the specified skew bucket, maintaining LIFO ordering for efficient access.

A key responsibility of this function is memory management. It tracks both total space usage and skew-specific space usage, triggering cleanup actions when limits are exceeded. If skew space exceeds the allowed limit, it removes the least important skew buckets. If total space exceeds the allowed limit, it increases the number of batches to reduce memory pressure.

The function matches the behavior of the current-batch case in ExecHashTableInsert but is specialized for skew bucket insertion.

## Parameters / Member Variables
- : The HashJoinTable containing the skew buckets and memory tracking information
- : TupleTableSlot containing the tuple data to be inserted
- : The 32-bit hash value of the tuple (for verification and storage)
- : Index of the specific skew bucket to insert the tuple into

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotMinimalTuple](ExecFetchSlotMinimalTuple.md) (extracts compact tuple representation)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (allocates memory for HashJoinTuple)
  - HeapTupleHeaderClearMatch (clears match flags in tuple header)
  - [ExecHashRemoveNextSkewBucket](ExecHashRemoveNextSkewBucket.md) (removes skew buckets when space limit exceeded)
  - [ExecHashIncreaseNumBatches](ExecHashIncreaseNumBatches.md) (increases batching when total space limit exceeded)
  - [heap_free_minimal_tuple](../h/heap_free_minimal_tuple.md) (frees temporary tuple if needed)
- Called from:
  - [MultiExecPrivateHash](../M/MultiExecPrivateHash.md) (during hash table population phase)

## Notes and Other Information
- Inserts tuples at the front of the skew bucket's linked list for LIFO access pattern
- Maintains both total space usage (spaceUsed) and skew-specific usage (spaceUsedSkew) counters
- Automatically triggers skew bucket removal when skew space limit is exceeded
- Can trigger batch number increase when total space limit is exceeded
- Uses batch context for memory allocation to ensure cleanup after batch completion
- Includes assertion to prevent circular references in the linked list
- Handles both owned and borrowed MinimalTuple scenarios with shouldFree flag
- Space tracking includes both the HashJoinTuple overhead and the actual tuple data size