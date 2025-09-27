# smgrdestroyall

## Location
[src/backend/storage/smgr/smgr.c:332-352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L332-L352)

## Overview
Destroys all unpinned SMgrRelation objects, releasing all their resources and removing them from storage manager data structures.

## Definition

```c
void
smgrdestroyall(void)
```
## Detailed Description
The  function performs a comprehensive cleanup of all unpinned SMgrRelation objects in the storage manager. It iterates through the global list of unpinned relations () and calls  on each one. This function is typically used during major cleanup operations like transaction end, checkpointing, or background writer maintenance. The function assumes that no external pointers to unpinned SMgrRelations exist except those explicitly pinned with .

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach_modify (safely iterates through the unpinned relations list)
  - dlist_container (extracts SMgrRelation from list node)
  - [smgrdestroy](smgrdestroy.md) (destroys individual relations)
  - [SMgrRelationData](../S/SMgrRelationData.md) (relation structure type)
  - unpinned_relns (global list of unpinned relations)
- Called from (representative examples):
  - [XLogDropDatabase](../X/XLogDropDatabase.md)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md)
  - [CheckpointerMain](../C/CheckpointerMain.md)
  - [RequestCheckpoint](../R/RequestCheckpoint.md)
  - [AtEOXact_SMgr](../A/AtEOXact_SMgr.md)

## Notes and Other Information
- This is a public function available to other modules
- Uses dlist_foreach_modify for safe iteration since smgrdestroy() modifies the list
- The function relies on smgrdestroy() to remove each relation from the list during iteration
- Called during critical system operations like transaction cleanup and checkpointing
- Only affects unpinned relations; pinned relations remain untouched
- The function assumes that all external references to unpinned relations have been properly cleared
- Essential for preventing resource leaks in long-running PostgreSQL processes
- Part of the storage manager's resource management strategy for maintaining system performance

## Simplified Source

```c
// Simplified version of smgrdestroyall
void smgrdestroyall(void) {
    dlist_mutable_iter iterator;

    // Iterate through all unpinned storage manager relations
    // Use safe iteration since smgrdestroy() modifies the list
    dlist_foreach_modify(iterator, &unpinned_relns) {
        // Extract the SMgrRelation from the list node
        SMgrRelation relation = dlist_container(SMgrRelationData, node, iterator.cur);

        // Destroy this relation (automatically removes it from the list)
        smgrdestroy(relation);
    }
}
```

Key simplifications made:
- Added descriptive comments explaining each step
- Used more descriptive variable names (iterator, relation)
- Focused on the core algorithm: iterate and destroy
- Emphasized the safe iteration pattern used for list modification