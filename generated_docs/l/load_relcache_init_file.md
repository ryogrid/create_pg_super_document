# load_relcache_init_file

## Location
src/backend/utils/cache/relcache.c: 6075 - 6490

## Overview
Attempts to load relation cache entries from the shared or local cache initialization file, enabling fast startup by avoiding sequential scans of system catalogs.

## Definition


## Detailed Description
This function is part of PostgreSQL's relation cache initialization optimization system. During normal backend startup, instead of building critical relation descriptors through expensive sequential scans of system catalogs, this function attempts to load pre-built relation cache entries from a binary initialization file.

The function handles both shared catalogs (global initialization file) and local database catalogs (database-specific initialization file). If successful, it populates the relation cache with critical relation descriptors including system tables and indexes, significantly speeding up backend startup.

The function performs extensive validation while reading the file, including magic number checks, structure size verification, and ensuring the correct number of nailed (critical) relations and indexes are loaded. If any validation fails, the function returns false, forcing the system to rebuild the cache the hard way.

For index relations, the function reconstructs complex index-specific data structures including operator families, operator input types, support procedures, collations, and options. For table relations, it initializes table access method data.

## Parameters / Member Variables
- : Boolean flag indicating whether to load the shared initialization file (for shared catalogs) or the local initialization file (for database-specific catalogs)

## Dependencies
- Functions called/Symbols referenced:
  - AllocateFile
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)  
  - [InitIndexAmRoutine](../I/InitIndexAmRoutine.md)
  - [RelationInitTableAccessMethod](../R/RelationInitTableAccessMethod.md)
  - [RelationInitLockInfo](../R/RelationInitLockInfo.md)
  - [RelationInitPhysicalAddr](../R/RelationInitPhysicalAddr.md)
  - RelationCacheInsert
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)/MemoryContextAllocZero
  - AllocSetContextCreate
- Called from (representative examples):
  - RelationCacheInitializePhase2
  - RelationCacheInitializePhase3

## Notes and Other Information
- The function operates in CacheMemoryContext and assumes this context is already active
- Sets global flags  or  on success
- Uses magic number RELCACHE_INIT_FILEMAGIC for file format validation
- Validates the count of nailed relations/indexes against expected constants (NUM_CRITICAL_SHARED_RELS, etc.)
- Complex data like rules, triggers, RLS policies, and partition info are not saved in the init file and must be rebuilt separately
- The init file mechanism significantly improves startup performance for databases with many system catalog entries
- File location: src/backend/utils/cache/relcache.c:6075-6490