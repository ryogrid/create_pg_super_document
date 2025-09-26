# GetBulkInsertState

## Location
[src/backend/access/heap/heapam.c:1971-1987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1971-L1987)

## Overview
GetBulkInsertState creates and initializes a BulkInsertState object that manages buffer allocation strategy and block tracking for efficient bulk insert operations.

## Definition

```c
BulkInsertState
GetBulkInsertState(void)
```
## Detailed Description
GetBulkInsertState allocates and initializes a new BulkInsertState structure used to optimize bulk insertion operations in PostgreSQL. The function sets up a bulk write access strategy and initializes tracking variables for buffer management during large-scale insert operations. This state object helps minimize buffer pool churn and improves performance by coordinating buffer allocation and reuse patterns during bulk inserts.

The function creates a state object that tracks the current buffer being used for inserts, maintains information about free blocks, and manages extension of relations during bulk operations.

## Parameters / Member Variables
This function takes no parameters and returns a BulkInsertState object with the following initialized members:
- : Set to BAS_BULKWRITE access strategy for optimized bulk operations
- : Initialized to InvalidBuffer, will track the current buffer in use
- : Initialized to InvalidBlockNumber, tracks next available free block
- : Initialized to InvalidBlockNumber, tracks last known free block
- : Initialized to 0, tracks relation extension count

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [GetAccessStrategy](GetAccessStrategy.md)
  - BAS_BULKWRITE
  - [BulkInsertStateData](../B/BulkInsertStateData.md)
  - InvalidBuffer
  - InvalidBlockNumber
- Called from (representative examples):
  - [CopyMultiInsertBufferInit](../C/CopyMultiInsertBufferInit.md)
  - [CopyFrom](../C/CopyFrom.md)
  - [intorel_startup](../i/intorel_startup.md)
  - [transientrel_startup](../t/transientrel_startup.md)
  - [ATRewriteTable](../A/ATRewriteTable.md)

## Notes and Other Information
- The returned BulkInsertState must be freed using FreeBulkInsertState when no longer needed
- The BAS_BULKWRITE strategy helps optimize buffer replacement during bulk operations
- This is typically used in conjunction with heap_insert and related bulk insertion functions
- The state object helps coordinate buffer management across multiple insert operations