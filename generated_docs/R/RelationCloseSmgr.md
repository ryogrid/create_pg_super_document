# RelationCloseSmgr

## Location
[src/include/utils/rel.h:582-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/rel.h#L582-L600)

## Overview
Closes the storage manager (smgr) level access for a relation, cleaning up resources and resetting the smgr handle to NULL.

## Definition
```c
static inline void
RelationCloseSmgr(Relation relation)
```

## Detailed Description
RelationCloseSmgr is an inline function that safely closes the storage manager handle for a relation. It performs the necessary cleanup operations by first unpinning the smgr handle (using smgrunpin) to release the pin that was set when the smgr was opened, then closing the storage manager (using smgrclose), and finally setting the relation's rd_smgr field to NULL to indicate that the smgr is no longer available.

The function includes a NULL check to ensure it only performs cleanup operations when the smgr handle actually exists, making it safe to call multiple times on the same relation.

## Parameters / Member Variables
- `relation`: The relation descriptor whose storage manager handle should be closed

## Dependencies
- Functions called/Symbols referenced:
  - [smgrunpin](../s/smgrunpin.md): Unpins the storage manager relation, releasing the pin reference
  - [smgrclose](../s/smgrclose.md): Closes the storage manager relation and releases associated resources
- Called from (representative examples):
  - [RelationDropStorage](RelationDropStorage.md): Storage cleanup during relation drop
  - [RelationReloadIndexInfo](RelationReloadIndexInfo.md): Index information reloading cleanup
  - [RelationDestroyRelation](RelationDestroyRelation.md): Relation destruction cleanup
  - [RelationInvalidateRelation](RelationInvalidateRelation.md): Relation invalidation cleanup  
  - [RelationClearRelation](RelationClearRelation.md): General relation cleanup
  - [RelationCacheInvalidate](RelationCacheInvalidate.md): Relation cache invalidation

## Notes and Other Information
- This is an inline function defined in rel.h for performance reasons
- Safe to call multiple times on the same relation due to NULL checking
- Must be called to properly clean up storage manager resources
- Typically called during relation cache invalidation, relation destruction, or storage cleanup operations
- The function ensures proper resource management by unpinning before closing
- After calling this function, any subsequent access to the relation's storage will require reopening the smgr via RelationGetSmgr
- Essential for preventing resource leaks in the storage manager subsystem

## Simplified Source

```c
static inline void
RelationCloseSmgr(Relation relation)
{
    // Only close if smgr handle exists
    if (relation->rd_smgr != NULL)
    {
        // Unpin the storage manager relation
        smgrunpin(relation->rd_smgr);

        // Close the storage manager handle
        smgrclose(relation->rd_smgr);

        // Clear the handle to indicate it's closed
        relation->rd_smgr = NULL;
    }
}
```