# RelationGetBufferForTuple

## Location
[src/backend/access/heap/hio.c:502-885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/hio.c#L502-L885)

## Overview
RelationGetBufferForTuple finds and returns a pinned, exclusive-locked buffer containing a page with sufficient free space for tuple insertion, handling complex buffer coordination, visibility map management, and relation extension.

## Definition

```c
Buffer
RelationGetBufferForTuple(Relation relation, Size len,
						  Buffer otherBuffer, int options,
						  BulkInsertState bistate,
						  Buffer *vmbuffer, Buffer *vmbuffer_other,
						  int num_pages)
```
## Detailed Description
This is a comprehensive function responsible for obtaining a suitable buffer for heap tuple insertion. It implements sophisticated logic for:

**Buffer Selection Strategy:**
- First tries cached target page from BulkInsertState or relation cache
- Falls back to Free Space Map (FSM) for finding pages with adequate space
- Attempts the last page of relation before extending
- Extends relation when no existing page has sufficient space

**Locking and Deadlock Prevention:**
- Handles complex buffer locking scenarios with proper ordering (ascending page numbers)
- Coordinates with otherBuffer to prevent deadlocks in concurrent operations
- Manages visibility map pins that must be acquired before buffer locks

**Space Management:**
- Respects fillfactor settings while allowing large tuples in nearly-empty pages
- Updates FSM with actual page free space information
- Supports bulk extension for efficient multi-page allocation

**Special Features:**
- HEAP_INSERT_SKIP_FSM option for bypassing FSM during bulk loads
- HEAP_INSERT_FROZEN support for frozen tuple insertion
- Bulk insert optimization through BulkInsertState caching
- Proper handling of all-visible page flag clearing

## Parameters / Member Variables
- : Target relation for tuple insertion
- : Required free space for the new tuple (will be MAXALIGN'd)
- : Previously pinned buffer for deadlock prevention (InvalidBuffer if none)
- : Insertion options (HEAP_INSERT_SKIP_FSM, HEAP_INSERT_FROZEN, etc.)
- : Bulk insert state for optimization (NULL for single inserts)
- : Input/output parameter for visibility map buffer of target page
- : Input/output parameter for visibility map buffer of otherBuffer
- : Number of pages to extend relation by if extension is needed (minimum 1)

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBufferBI](ReadBufferBI.md), ReadBuffer, ReadBufferExtended
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md), BufferGetPage, BufferGetPageSize
  - [PageIsAllVisible](../P/PageIsAllVisible.md), PageIsNew, PageInit, PageGetHeapFreeSpace, PageGetMaxOffsetNumber
  - [GetVisibilityMapPins](../G/GetVisibilityMapPins.md), visibilitymap_pin, visibilitymap_pin_ok
  - [LockBuffer](../L/LockBuffer.md), ConditionalLockBuffer, ReleaseBuffer, UnlockReleaseBuffer
  - RelationGetTargetBlock, RelationSetTargetBlock, RelationAddBlocks
  - [GetPageWithFreeSpace](../G/GetPageWithFreeSpace.md), RecordPageWithFreeSpace, RecordAndGetPageWithFreeSpace
  - RelationGetTargetPageFreeSpace, RelationGetNumberOfBlocks
- Called from:
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)  
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- Central function in PostgreSQL's heap insertion mechanism
- Implements careful lock ordering to prevent deadlocks (buffers locked in ascending page order)
- The function can release and reacquire locks during visibility map pin operations
- Handles race conditions where page state changes during lock/pin operations
- Supports both single tuple insertion and bulk insert optimization
- EREPORT(ERROR) is allowed, unlike lower-level functions like RelationPutHeapTuple
- [Complex](../C/Complex.md) retry logic handles cases where target buffer state changes during processing
- The function maintains relation's target block cache for insertion locality