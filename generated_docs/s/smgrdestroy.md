# smgrdestroy

## Location
src/backend/storage/smgr/smgr.c: 277 - 299

## Overview
Deletes an SMgrRelation object, cleaning up its resources and removing it from the storage manager's hash table.

## Definition


## Detailed Description
The  function is responsible for properly destroying an SMgrRelation object. It performs a complete cleanup by first closing all fork files associated with the relation, then removing the relation from the doubly-linked list of SMgrRelation objects, and finally removing it from the SMgrRelationHash hash table. This function ensures that all resources are properly released and that the storage manager's internal data structures remain consistent.

## Parameters / Member Variables
- : Pointer to the SMgrRelation object to be destroyed. The relation must have a pincount of 0 (not in use by any backend).

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