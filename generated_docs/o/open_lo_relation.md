# open_lo_relation

## Location
[src/backend/storage/large_object/inv_api.c:74-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L74-L97)

## Overview
Opens the PostgreSQL large object relation (pg_largeobject) and its associated index if they are not already open in the current transaction.

## Definition

```c
static void
open_lo_relation(void)
```
## Detailed Description
This internal function ensures that the large object heap relation and its primary index are available for operations within the current transaction. It uses a lazy initialization approach, only opening the relations if they haven't been opened already. The function acquires RowExclusiveLock on both the table and index to allow both read and write operations. To ensure proper resource management, it temporarily switches the resource owner to TopTransactionResourceOwner so that the relation references are owned by the top-level transaction.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (via LargeObjectRelationId)
  - [index_open](../i/index_open.md) (via LargeObjectLOidPNIndexId) 
  - [ResourceOwner](../R/ResourceOwner.md) (resource management)
- Called from (representative examples):
  - [inv_getsize](../i/inv_getsize.md)
  - [inv_read](../i/inv_read.md)
  - [inv_write](../i/inv_write.md) 
  - [inv_truncate](../i/inv_truncate.md)

## Notes and Other Information
- Function is static (internal to inv_api.c)
- Uses global variables lo_heap_r and lo_index_r to track relation state
- Employs RowExclusiveLock for both relations to support read/write operations
- Resource ownership is temporarily transferred to TopTransactionResourceOwner for proper cleanup
- Implements lazy initialization pattern for performance

## Simplified Source

```c
static void open_lo_relation(void) {
    // Skip if relations already open in current transaction
    if (lo_heap_r && lo_index_r)
        return;

    // Temporarily switch to top transaction resource owner for proper cleanup
    ResourceOwner currentOwner = CurrentResourceOwner;
    CurrentResourceOwner = TopTransactionResourceOwner;

    // Open large object table and index with RowExclusiveLock for read/write access
    if (lo_heap_r == NULL)
        lo_heap_r = table_open(LargeObjectRelationId, RowExclusiveLock);
    if (lo_index_r == NULL)
        lo_index_r = index_open(LargeObjectLOidPNIndexId, RowExclusiveLock);

    // Restore original resource owner
    CurrentResourceOwner = currentOwner;
}
```