# raw_heap_insert

## Location
[src/backend/access/heap/rewriteheap.c:593-758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L593-L758)

## Overview
Inserts a tuple into the new heap relation during a rewrite operation, handling TOAST processing, page management, and efficient bulk writing without WAL logging or visibility checks.

## Definition

```c
static void
raw_heap_insert(RewriteState state, HeapTuple tup)
```
## Detailed Description
The `raw_heap_insert` function performs low-level tuple insertion during heap rewrite operations, bypassing normal heap insertion mechanisms like WAL logging, visibility checks, and FSM (Free Space Map) updates. It handles TOAST processing for oversized tuples, manages page allocation and writing through the bulk write interface, and maintains proper tuple positioning and cross-references.

The function operates in bulk mode, accumulating tuples in memory-buffered pages and writing complete pages to disk when full. It automatically handles TOAST processing when tuples exceed the threshold or contain external references, with special handling for TOAST table entries themselves. The function ensures proper CTID (Current Tuple Identifier) setup for both the caller's tuple reference and the stored tuple data.

## Parameters / Member Variables
- `state`: The RewriteState structure containing bulk write context, buffer management, and relation references
- `tup`: The HeapTuple to insert into the new relation (t_self will be updated to reflect actual storage location)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHasExternal
  - [heap_toast_insert_or_update](../h/heap_toast_insert_or_update.md)
  - RelationGetTargetPageFreeSpace
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md)
  - [smgr_bulk_write](../s/smgr_bulk_write.md)
  - [smgr_bulk_get_buf](../s/smgr_bulk_get_buf.md)
  - [PageInit](../P/PageInit.md)
  - PageAddItem
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [end_heap_rewrite](../e/end_heap_rewrite.md)
  - [rewrite_heap_tuple](rewrite_heap_tuple.md)

## Notes and Other Information
- Operates without WAL logging, FSM updates, or visibility checks for maximum performance during rewrites
- Uses bulk write interface to efficiently write complete pages rather than individual tuples
- Automatically invokes TOAST processing for oversized tuples or those with external attributes
- Prevents logical decoding of TOAST data during VACUUM FULL/CLUSTER operations
- Respects relation fillfactor settings when determining page space utilization
- Updates both caller's t_self and stored tuple's t_ctid to maintain proper tuple references
- Handles memory management for TOAST-processed tuples to prevent leaks
- Enforces maximum tuple size limits and provides detailed error messages for oversized tuples
- Part of the high-performance rewrite infrastructure that bypasses normal heap access methods