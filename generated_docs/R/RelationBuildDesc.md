# RelationBuildDesc

## Location
src/backend/utils/cache/relcache.c: 1040 - 1319

## Overview
Builds a complete relation descriptor from scratch by reading system catalogs and initializing all necessary components of the relation cache entry.

## Definition


## Detailed Description
This is a core function that constructs a complete Relation structure by reading the pg_class tuple for a given relation OID and initializing all its components. The function performs extensive setup including memory management, tuple descriptor construction, access method initialization, rules/triggers/row security loading, and physical addressing setup.

The function includes sophisticated memory management with optional temporary context creation to prevent memory leaks during debug operations. It handles invalidation detection during build process and implements a retry mechanism. The function can optionally insert the completed relation into the relation cache hash table.

## Parameters / Member Variables
- : OID of the relation to build descriptor for
- : Whether to insert the completed relation into the cache hash table

## Dependencies
- Functions called/Symbols referenced:
  - [ScanPgRelation](../S/ScanPgRelation.md) (scan pg_class for relation tuple)
  - [AllocateRelationDesc](../A/AllocateRelationDesc.md) (allocate relation structure)
  - [RelationBuildTupleDesc](RelationBuildTupleDesc.md) (build tuple descriptor)
  - [RelationInitIndexAccessInfo](RelationInitIndexAccessInfo.md) (initialize index access methods)
  - [RelationInitTableAccessMethod](RelationInitTableAccessMethod.md) (initialize table access methods)
  - [RelationParseRelOptions](RelationParseRelOptions.md) (parse relation options)
  - [RelationBuildRuleLock](RelationBuildRuleLock.md) (build rule locks)
  - [RelationBuildTriggers](RelationBuildTriggers.md) (build trigger information)
  - [RelationBuildRowSecurity](RelationBuildRowSecurity.md) (build row security policies)
  - [RelationInitLockInfo](RelationInitLockInfo.md) (initialize lock manager info)
  - [RelationInitPhysicalAddr](RelationInitPhysicalAddr.md) (initialize physical addressing)
  - RelationCacheInsert (insert into cache if requested)
  - [heap_freetuple](../h/heap_freetuple.md) (free pg_class tuple)
- Called from (representative examples):
  - [RelationIdGetRelation](RelationIdGetRelation.md)
  - [RelationClearRelation](RelationClearRelation.md)
  - [load_critical_index](../l/load_critical_index.md)

## Notes and Other Information
- Requires caller to hold at least AccessShareLock on target relation
- Returns NULL if pg_class tuple not found (relation may have been deleted)
- Implements memory recovery mechanism when debug_discard_caches is active
- Maintains in_progress_list to track invalidations during build process
- Handles different relation persistence types (permanent, unlogged, temporary)
- Properly sets up backend ownership for temporary relations
- Implements retry mechanism if invalidation occurs during build
- Initializes all relation descriptor fields including reference counts, subtransaction IDs, and validity flags
- Critical component of PostgreSQL's relation cache system