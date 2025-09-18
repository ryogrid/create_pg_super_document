# perform_relmap_update

## Location
[src/backend/utils/cache/relmapper.c:1039-1095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L1039-L1095)

## Overview
perform_relmap_update applies pending relation mapping changes to the persistent storage by merging updates with current mappings and writing the results to disk.

## Definition


## Detailed Description
perform_relmap_update is the central function that commits relation mapping changes during normal multiuser operation. It implements a safe update protocol that includes acquiring exclusive locks, re-reading current mappings to ensure consistency, merging pending updates, writing the new mappings to disk, and updating in-memory structures.

The function ensures atomicity and consistency by using RelationMappingLock to prevent concurrent updates, re-reading the mapping file to capture any recent changes from other processes, applying the updates through merge_map_updates, and only updating the in-memory structures after successful disk writes. This protocol prevents race conditions and ensures all processes see consistent mapping information.

## Parameters / Member Variables
- : Boolean indicating whether to update shared mappings (true) or local database mappings (false)
- : Pointer to RelMapFile structure containing the pending updates to apply

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease (exclusive locking)
  - [load_relmap_file](../l/load_relmap_file.md) (re-read current mappings from disk)
  - memcpy (copy mapping structures)
  - [merge_map_updates](../m/merge_map_updates.md) (apply updates to mapping data)
  - [write_relmap_file](../w/write_relmap_file.md) (write updated mappings to disk)
  - shared_map/local_map (global mapping structures)
  - allowSystemTableMods (global flag)
  - MyDatabaseId/MyDatabaseTableSpace/DatabasePath (global database context)
- Called from (representative examples):
  - [AtEOXact_RelationMap](../A/AtEOXact_RelationMap.md) (at src/backend/utils/cache/relmapper.c:558, 563)

## Notes and Other Information
- This is a static function, only accessible within the relmapper.c file
- Must be used for committing updates during normal multiuser operation
- Assumes callers hold exclusive locks on affected relations until commit
- Re-reads mapping files to ensure consistency with concurrent updates
- Updates are applied through merge_map_updates with system table modification checks
- In-memory structures are updated only after successful disk writes
- Uses write_relmap_file with full WAL logging, invalidation, and file preservation
- Part of PostgreSQL's transactional relation mapping commit protocol
- Called during transaction end-of-transaction processing