# smgrdestroy

## Location
[src/backend/storage/smgr/smgr.c:277-299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L277-L299)

## Overview
Deletes an SMgrRelation object, cleaning up its resources and removing it from the storage manager's hash table.

## Definition

```c
static void
smgrdestroy(SMgrRelation reln)
```
## Detailed Description
The  function is responsible for properly destroying an SMgrRelation object. It performs a complete cleanup by first closing all fork files associated with the relation, then removing the relation from the doubly-linked list of SMgrRelation objects, and finally removing it from the SMgrRelationHash hash table. This function ensures that all resources are properly released and that the storage manager's internal data structures remain consistent.

## Parameters / Member Variables
- `reln`: Pointer to the SMgrRelation object to be destroyed. The relation must have a pincount of 0 (not in use by any backend).
## Dependencies
- Functions called/Symbols referenced:
  - smgrsw[].smgr_close (closes file descriptors for all forks)
  - [dlist_delete](../d/dlist_delete.md) (removes from doubly-linked list)  
  - [hash_search](../h/hash_search.md) (removes from hash table with HASH_REMOVE)
  - elog (error logging)
- Called from (representative examples):
  - [smgrdestroyall](smgrdestroyall.md)

## Notes and Other Information
- This is a static function, only callable within the smgr.c file
- The function asserts that reln->pincount == 0, meaning the relation must not be in active use
- It systematically closes all fork files (0 to MAX_FORKNUM) before cleanup
- If the hash table removal fails, it triggers an ERROR indicating hash table corruption
- The function maintains the integrity of both the doubly-linked list and hash table data structures

## Simplified Source

```c
// Simplified version of smgrdestroy
static void smgrdestroy(SMgrRelation reln) {
    // Verify relation is not in use
    Assert(reln->pincount == 0);

    // Close all fork files for this relation
    for (ForkNumber forknum = 0; forknum <= MAX_FORKNUM; forknum++) {
        smgrsw[reln->smgr_which].smgr_close(reln, forknum);
    }

    // Remove from doubly-linked list
    dlist_delete(&reln->node);

    // Remove from hash table
    if (hash_search(SMgrRelationHash, &(reln->smgr_rlocator), HASH_REMOVE, NULL) == NULL) {
        elog(ERROR, "SMgrRelation hashtable corrupted");
    }
}
```

Key simplifications made:
- Preserved the essential cleanup sequence: close files → remove from list → remove from hash
- Kept critical error checking for hash table corruption
- Added descriptive comments for each cleanup step
- Maintained the exact logic flow of the original function