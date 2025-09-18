# open_lo_relation

## Location
src/backend/storage/large_object/inv_api.c: 74 - 97

## Overview
Opens the PostgreSQL large object relation (pg_largeobject) and its associated index if they are not already open in the current transaction.

## Definition


## Detailed Description
This internal function ensures that the large object heap relation and its primary index are available for operations within the current transaction. It uses a lazy initialization approach, only opening the relations if they haven't been opened already. The function acquires RowExclusiveLock on both the table and index to allow both read and write operations. To ensure proper resource management, it temporarily switches the resource owner to TopTransactionResourceOwner so that the relation references are owned by the top-level transaction.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - table_open (via LargeObjectRelationId)
  - index_open (via LargeObjectLOidPNIndexId) 
  - ResourceOwner (resource management)
- Called from (representative examples):
  - inv_getsize
  - inv_read
  - inv_write 
  - inv_truncate

## Notes and Other Information
- Function is static (internal to inv_api.c)
- Uses global variables lo_heap_r and lo_index_r to track relation state
- Employs RowExclusiveLock for both relations to support read/write operations
- Resource ownership is temporarily transferred to TopTransactionResourceOwner for proper cleanup
- Implements lazy initialization pattern for performance