# AssertHasSnapshotForToast

## Location
[src/backend/access/heap/heapam.c:238-275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L238-L275)

## Overview
A debug assertion function that verifies a valid snapshot exists before performing operations that might require TOAST table access.

## Definition

```c
static inline void
AssertHasSnapshotForToast(Relation rel)
```
## Detailed Description
AssertHasSnapshotForToast is a debug-only assertion function (active only when USE_ASSERT_CHECKING is defined) that ensures a valid snapshot is available before performing heap operations that might need to access TOAST tables. TOAST (The Oversized-Attribute Storage Technique) is PostgreSQL's mechanism for storing large attribute values separately from the main table.

The function performs several checks:
1. Skips validation during bootstrap mode when normal snapshot rules don't apply
2. Returns early if the relation has no associated TOAST table
3. Makes a special exception for pg_replication_origin relation due to historical compatibility issues
4. Asserts that an active or registered snapshot exists using HaveRegisteredOrActiveSnapshot()

This validation prevents crashes that could occur when trying to detoast (decompress/retrieve) large values without a proper snapshot context.

## Parameters / Member Variables
- : The relation being accessed, used to check if it has an associated TOAST table and to identify special cases like pg_replication_origin

## Dependencies
- Functions called/Symbols referenced:
  - IsNormalProcessingMode
  - OidIsValid
  - RelationGetRelid
  - HaveRegisteredOrActiveSnapshot
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md) (src/backend/access/heap/heapam.c:2051)
  - [heap_multi_insert](../h/heap_multi_insert.md) (src/backend/access/heap/heapam.c:2330)
  - [heap_delete](../h/heap_delete.md) (src/backend/access/heap/heapam.c:2754)
  - [heap_update](../h/heap_update.md) (src/backend/access/heap/heapam.c:3251)

## Notes and Other Information
- This is a debug-only function that compiles to nothing in production builds (when USE_ASSERT_CHECKING is not defined)
- The special case for pg_replication_origin is a deliberate exception for backward compatibility on older branches
- The assertion helps catch programming errors where heap modification operations are attempted without proper snapshot context
- TOAST access requires snapshots because it may need to read data that should be consistent with the current transaction's view