# CacheInvalidateRelmap

## Location
[src/backend/utils/cache/inval.c:1492-1518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L1492-L1518)

## Overview
Registers invalidation of the relation mapping for a database or shared catalogs, forcing other backends to re-read the relation mapping file.

## Definition
```c
void CacheInvalidateRelmap(Oid databaseId)
```

## Detailed Description
CacheInvalidateRelmap sends an invalidation message that forces other PostgreSQL backends to re-read the relation mapping file for a specified database. The relation mapping file contains the mapping between logical relation identifiers and their physical file locations on disk.

Key characteristics:
- **Database-specific or shared**: When databaseId is zero, it invalidates shared catalog mappings; otherwise, it invalidates mappings for the specific database
- **Nontransactional**: Like smgr invalidations, these messages are sent immediately without queuing since they relate to low-level file system operations
- **WAL independence**: These messages are not captured in commit/abort WAL entries and should be called from low-level relmapper.c routines
- **Requires additional relcache invalidation**: It's necessary to also send relcache invalidation for specific relations whose mapping has changed to ensure the relcache gets updated with new filenode data

The function constructs a SharedInvalidationMessage with the SHAREDINVALRELMAP_ID identifier and the target database ID, then sends it immediately to all backends.

## Parameters / Member Variables
- `databaseId`: OID of the database whose relation mapping should be invalidated. When set to zero, invalidates shared catalog mappings instead of database-specific mappings

## Dependencies
- Functions called/Symbols referenced:
  - SharedInvalidationMessage (message structure)
  - SHAREDINVALRELMAP_ID (message type identifier)
  - VALGRIND_MAKE_MEM_DEFINED (memory debugging macro)
  - [SendSharedInvalidMessages](../S/SendSharedInvalidMessages.md) (message transmission function)
- Called from (representative examples):
  - [write_relmap_file](../w/write_relmap_file.md) (src/backend/utils/cache/relmapper.c:1001)

## Notes and Other Information
- Must be accompanied by relcache invalidation messages for the specific relations affected to ensure complete cache coherency
- Should be called from relmapper.c routines that execute during both WAL creation and replay
- Essential for maintaining consistency when relation-to-filenode mappings change, particularly for system catalogs
- The immediate sending ensures that all backends have consistent view of relation file mappings after changes
- Used primarily when the relation mapper writes updated mapping files to disk