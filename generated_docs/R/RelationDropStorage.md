# RelationDropStorage

## Location
[src/backend/catalog/storage.c:206-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L206-L250)

## Overview
RelationDropStorage schedules the physical deletion of relation storage files at transaction commit time, ensuring proper cleanup while maintaining transaction safety.

## Definition
```c
void RelationDropStorage(Relation rel)
```

## Detailed Description
RelationDropStorage schedules the unlinking of physical storage files for a relation to occur at transaction commit time. Rather than immediately deleting the files, it adds the relation to a pending deletion list that will be processed when the transaction commits successfully. This approach ensures transaction safety - if the transaction aborts, the files remain intact. The function handles the case where a relation was created and dropped within the same transaction by allowing duplicate entries in the pending list (one for commit deletion, one for abort deletion), with the storage manager logic handling the redundancy appropriately. After scheduling deletion, it closes the storage manager handle for the relation.

## Parameters / Member Variables
- `rel`: Pointer to Relation structure representing the relation whose storage should be dropped, containing file locator and backend process information

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - RelationCloseSmgr
  - [PendingRelDelete](../P/PendingRelDelete.md)
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [index_drop](../i/index_drop.md)
  - [RelationSetNewRelfilenumber](RelationSetNewRelfilenumber.md)
  - [reindex_index](../r/reindex_index.md)

## Notes and Other Information
- Creates PendingRelDelete entry in TopMemoryContext with atCommit=true to ensure deletion on commit
- Allows duplicate entries for relations created and dropped in same transaction without error
- The actual file deletion is deferred to smgrDoPendingDeletes at transaction end
- Closes storage manager handle immediately to prevent further access to the relation files
- Uses the relations rd_locator and rd_backend fields to identify files for deletion
- Transaction nesting level is recorded to handle subtransaction rollback scenarios properly