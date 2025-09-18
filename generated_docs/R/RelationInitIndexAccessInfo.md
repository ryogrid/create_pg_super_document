# RelationInitIndexAccessInfo

## Location
src/backend/utils/cache/relcache.c: 1426 - 1596

## Overview
RelationInitIndexAccessInfo initializes comprehensive index access method support data for an index relation, including access method routines, operator classes, support functions, and index metadata.

## Definition
```c
void RelationInitIndexAccessInfo(Relation relation)
```

## Detailed Description
This function performs complete initialization of index access method support data for an index relation. It retrieves and caches the pg_index entry, looks up the access method handler, creates a memory context for index-specific data, initializes the IndexAmRoutine structure, and sets up arrays for operator families, operator classes, support functions, collations, and options. The function also extracts variable-length fields from the pg_index tuple (indcollation, indclass, indoption) and initializes support procedure information.

## Parameters / Member Variables
- `relation`: The index relation to initialize. Must be a valid index relation with rd_rel->relam set to a valid access method OID.

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1, ReleaseSysCache
  - heap_copytuple
  - MemoryContextSwitchTo, AllocSetContextCreate, MemoryContextAllocZero, MemoryContextCopyAndSetIdentifier
  - RelationGetNumberOfAttributes, IndexRelationGetNumberOfAttributes, IndexRelationGetNumberOfKeyAttributes
  - InitIndexAmRoutine
  - fastgetattr, GetPgIndexDescriptor
  - IndexSupportInitialize
  - RelationGetIndexAttOptions
  - Form_pg_am, Form_pg_index, oidvector, int2vector, RegProcedure
- Called from:
  - index_create
  - RelationBuildDesc

## Notes and Other Information
- This function sets up the complete index access infrastructure needed for index operations
- Creates a dedicated memory context (rd_indexcxt) for index-specific allocations to prevent memory leaks
- Handles variable-length fields in pg_index by extracting them as Datum values rather than direct struct access
- Initializes support function arrays only if the access method requires support functions (amsupport > 0)
- The rd_indexprs, rd_indpred, rd_exclops, rd_exclprocs, rd_exclstrats, and rd_amcache fields are initialized as empty and filled later on demand
- Essential for index functionality as it bridges the gap between catalog information and runtime access method operations