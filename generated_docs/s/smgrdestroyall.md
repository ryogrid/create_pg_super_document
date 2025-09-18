# smgrdestroyall

## Location
src/backend/storage/smgr/smgr.c: 332 - 352

## Overview
Destroys all unpinned SMgrRelation objects, releasing all their resources and removing them from storage manager data structures.

## Definition


## Detailed Description
The  function performs a comprehensive cleanup of all unpinned SMgrRelation objects in the storage manager. It iterates through the global list of unpinned relations () and calls  on each one. This function is typically used during major cleanup operations like transaction end, checkpointing, or background writer maintenance. The function assumes that no external pointers to unpinned SMgrRelations exist except those explicitly pinned with .

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach_modify (safely iterates through the unpinned relations list)
  - dlist_container (extracts SMgrRelation from list node)
  - smgrdestroy (destroys individual relations)
  - SMgrRelationData (relation structure type)
  - unpinned_relns (global list of unpinned relations)
- Called from (representative examples):
  - XLogDropDatabase
  - BackgroundWriterMain
  - CheckpointerMain
  - RequestCheckpoint
  - AtEOXact_SMgr

## Notes and Other Information
- This is a public function available to other modules
- Uses dlist_foreach_modify for safe iteration since smgrdestroy() modifies the list
- The function relies on smgrdestroy() to remove each relation from the list during iteration
- Called during critical system operations like transaction cleanup and checkpointing
- Only affects unpinned relations; pinned relations remain untouched
- The function assumes that all external references to unpinned relations have been properly cleared
- Essential for preventing resource leaks in long-running PostgreSQL processes
- Part of the storage manager's resource management strategy for maintaining system performance