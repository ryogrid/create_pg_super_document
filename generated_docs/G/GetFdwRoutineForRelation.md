# GetFdwRoutineForRelation

## Location
src/backend/foreign/foreign.c: 442 - 481

## Overview
Retrieves the FdwRoutine structure for a foreign table with caching optimization, storing the result in the relation cache for future reuse.

## Definition
```c
FdwRoutine *GetFdwRoutineForRelation(Relation relation, bool makecopy)
```

## Detailed Description
This function is the preferred method for obtaining FdwRoutine structures because it implements intelligent caching to avoid repeated catalog lookups. When first called for a relation, it uses `GetFdwRoutineByRelId` to retrieve the FDW routine and stores a copy in the relation cache (rd_fdwroutine field). Subsequent calls return the cached version, significantly improving performance.

The function provides memory management flexibility through the `makecopy` parameter: when true, it returns a freshly allocated copy in the caller's memory context; when false, it returns a pointer to the cached data which is valid until the next relcache reset.

## Parameters / Member Variables
- `relation`: Pointer to the Relation structure for the foreign table
- `makecopy`: Boolean flag indicating whether to return a freshly allocated copy (true) or a pointer to cached data (false)

## Dependencies
- Functions called/Symbols referenced:
  - GetFdwRoutineByRelId
  - RelationGetRelid
  - MemoryContextAlloc
  - memcpy
  - palloc
- Called from (representative examples):
  - analyze_rel
  - acquire_inherited_sample_rows
  - CheckValidRowMarkRel
  - InitResultRelInfo
  - EvalPlanQualFetchRowMark
  - ExecInitForeignScan
  - ExecLockRows
  - add_row_identity_columns
  - get_relation_info

## Notes and Other Information
- Preferred over GetFdwRoutineByRelId due to caching optimization
- Uses CacheMemoryContext for storing cached FDW routine data
- First call performs catalog lookup and caches result in relation->rd_fdwroutine
- Subsequent calls return cached data, avoiding expensive catalog lookups
- makecopy=true: Returns freshly palloc'd copy safe for long-term use
- makecopy=false: Returns pointer to cached data, valid only until relcache reset
- Essential for performance in scenarios with repeated FDW operations on same table