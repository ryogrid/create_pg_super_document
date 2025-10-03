# RelationPutHeapTuple

## Location
[src/backend/access/heap/hio.c:35-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/hio.c#L35-L87)

## Overview
RelationPutHeapTuple places a heap tuple at a specified page in a PostgreSQL buffer, handling the physical insertion of tuple data into heap pages with proper offset tracking and CTID management.

## Definition

```c
void
RelationPutHeapTuple(Relation relation,
					 Buffer buffer,
					 HeapTuple tuple,
					 bool token)
```
## Detailed Description
This function performs the low-level physical insertion of a heap tuple into a buffer page. It is a critical component of PostgreSQL's heap access method, responsible for:
- Adding the tuple data to the specified page using PageAddItem
- Updating the tuple's t_self field with the actual storage position 
- Setting the correct CTID in the stored tuple header (unless it's a speculative insertion)
- Performing validation checks on tuple hint bits to prevent corruption

The function includes strict error handling - it must PANIC on failure rather than using EREPORT(ERROR), indicating this is used in contexts where partial failure is not acceptable. The caller must hold BUFFER_LOCK_EXCLUSIVE on the buffer before calling this function.

## Parameters / Member Variables
- `relation`: The relation (table) where the tuple is being inserted
- `buffer`: The buffer containing the target page (caller must hold BUFFER_LOCK_EXCLUSIVE)
- `tuple`: The heap tuple to be inserted into the page
- `token`: Boolean flag indicating whether this is a speculative insertion (token held in CTID field)
## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)  
  - PageAddItem
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - HeapTupleHeaderIsSpeculative
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- Critical constraint: EREPORT(ERROR) is disallowed - must PANIC on failure
- Requires caller to hold BUFFER_LOCK_EXCLUSIVE on the buffer
- Validates tuple hint bits to prevent corruption detectable by contrib/amcheck
- Handles both regular and speculative insertions differently for CTID management
- Updates both tuple->t_self and the stored tuple's t_ctid fields for proper tuple chain management
- Failure to add tuple to page results in PANIC, indicating a serious system-level error

## Simplified Source

```c
void RelationPutHeapTuple(Relation relation,
                          Buffer buffer,
                          HeapTuple tuple,
                          bool token)
{
    Page pageHeader;
    OffsetNumber offnum;

    // Validate speculative insertion token consistency
    Assert(!token || HeapTupleHeaderIsSpeculative(tuple->t_data));

    // Validate tuple hint bits to prevent corruption
    Assert(!((tuple->t_data->t_infomask & HEAP_XMAX_COMMITTED) &&
             (tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI)));

    // Add tuple to the page
    pageHeader = BufferGetPage(buffer);

    offnum = PageAddItem(pageHeader, (Item) tuple->t_data,
                         tuple->t_len, InvalidOffsetNumber, false, true);

    if (offnum == InvalidOffsetNumber)
        elog(PANIC, "failed to add tuple to page");

    // Update tuple's self-reference with actual storage position
    ItemPointerSet(&(tuple->t_self), BufferGetBlockNumber(buffer), offnum);

    // Set CTID in stored tuple (except for speculative insertions)
    if (!token)
    {
        ItemId itemId = PageGetItemId(pageHeader, offnum);
        HeapTupleHeader item = (HeapTupleHeader) PageGetItem(pageHeader, itemId);

        item->t_ctid = tuple->t_self;
    }
}
```