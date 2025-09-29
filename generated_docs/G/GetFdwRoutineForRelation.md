# GetFdwRoutineForRelation

## Location
[src/backend/foreign/foreign.c:442-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L442-L481)

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
  - [GetFdwRoutineByRelId](GetFdwRoutineByRelId.md)
  - RelationGetRelid
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - memcpy
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [analyze_rel](../a/analyze_rel.md)
  - [acquire_inherited_sample_rows](../a/acquire_inherited_sample_rows.md)
  - [CheckValidRowMarkRel](../C/CheckValidRowMarkRel.md)
  - [InitResultRelInfo](../I/InitResultRelInfo.md)
  - [EvalPlanQualFetchRowMark](../E/EvalPlanQualFetchRowMark.md)
  - [ExecInitForeignScan](../E/ExecInitForeignScan.md)
  - [ExecLockRows](../E/ExecLockRows.md)
  - [add_row_identity_columns](../a/add_row_identity_columns.md)
  - [get_relation_info](../g/get_relation_info.md)

## Notes and Other Information
- Preferred over GetFdwRoutineByRelId due to caching optimization
- Uses CacheMemoryContext for storing cached FDW routine data
- First call performs catalog lookup and caches result in relation->rd_fdwroutine
- Subsequent calls return cached data, avoiding expensive catalog lookups
- makecopy=true: Returns freshly palloc'd copy safe for long-term use
- makecopy=false: Returns pointer to cached data, valid only until relcache reset
- Essential for performance in scenarios with repeated FDW operations on same table

## Simplified Source

```c
FdwRoutine *
GetFdwRoutineForRelation(Relation relation, bool makecopy)
{
    FdwRoutine *fdwroutine;
    FdwRoutine *cached_routine;

    // Check if we already have cached FDW routine
    if (relation->rd_fdwroutine == NULL)
    {
        // First time: lookup FDW routine from catalogs
        fdwroutine = GetFdwRoutineByRelId(RelationGetRelid(relation));

        // Cache the routine in relation cache for future use
        cached_routine = (FdwRoutine *) MemoryContextAlloc(CacheMemoryContext,
                                                           sizeof(FdwRoutine));
        memcpy(cached_routine, fdwroutine, sizeof(FdwRoutine));
        relation->rd_fdwroutine = cached_routine;

        // Return the original copy
        return fdwroutine;
    }

    // We have cached data - check if caller wants a copy
    if (makecopy)
    {
        // Create fresh copy for caller
        fdwroutine = (FdwRoutine *) palloc(sizeof(FdwRoutine));
        memcpy(fdwroutine, relation->rd_fdwroutine, sizeof(FdwRoutine));
        return fdwroutine;
    }

    // Return pointer to cached data (valid until relcache reset)
    return relation->rd_fdwroutine;
}
```