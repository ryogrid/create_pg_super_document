# formrdesc

## Location
src/backend/utils/cache/relcache.c: 1875 - 2062

## Overview
A special cut-down version of RelationBuildDesc() used during relcache initialization to build relation descriptors from supplied parameters without accessing system tables.

## Definition


## Detailed Description
This function creates relation descriptors for basic system catalogs during PostgreSQL's bootstrap phase and early initialization. It builds the relation descriptor purely from the provided parameters without performing any system table lookups, making it suitable for use when the system catalogs themselves are not yet fully available.

The function handles the creation of "nailed" relations (permanently cached relations) and sets up minimal but sufficient information to get the system launched. The actual complete data will be filled in later by RelationCacheInitializePhase3().

Key characteristics of relations created by formrdesc:
- All are marked as nailed-in-cache with reference count 1
- All are permanent relations (RELPERSISTENCE_PERMANENT)
- All use heap table access method (HEAP_TABLE_AM_OID)
- All are mapped relations with invalid file numbers initially
- Cannot have constraints (except attnotnull), default values, rules, or triggers

## Parameters / Member Variables
- : Name of the relation to create
- : OID of the relation's composite type
- : Whether this is a shared relation (stored in global tablespace)
- : Number of attributes in the relation
- : Array of attribute definitions (FormData_pg_attribute)

## Dependencies
- Functions called/Symbols referenced:
  - CreateTemplateTupleDesc
  - RelationInitLockInfo
  - RelationInitPhysicalAddr
  - GetHeapamTableAmRoutine
  - RelationMapUpdateMap
  - RelationCacheInsert
  - IsBootstrapProcessingMode
  - namestrcpy
- Called from (representative examples):
  - RelationCacheInitializePhase2
  - RelationCacheInitializePhase3

## Notes and Other Information
- Only used for a few basic system catalogs during initialization
- Creates incomplete/bogus data that gets replaced later in RelationCacheInitializePhase3()
- The relowner field is left as zero to signal that real data isn't loaded yet
- All relations created are marked as having indexes (except in bootstrap mode)
- Assumes caller has already switched to CacheMemoryContext
- Part of PostgreSQL's three-phase relation cache initialization process
- Critical for bootstrap process when system catalogs are being created