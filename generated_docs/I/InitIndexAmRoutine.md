# InitIndexAmRoutine

## Location
[src/backend/utils/cache/relcache.c:1402-1425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L1402-L1425)

## Overview
InitIndexAmRoutine fills in the IndexAmRoutine structure for an index relation by calling the access method handler and caching the result in the relation's index context.

## Definition
```c
static void InitIndexAmRoutine(Relation relation)
```

## Detailed Description
This static function initializes the IndexAmRoutine structure for an index relation. It calls GetIndexAmRoutine() with the relation's access method handler to obtain the IndexAmRoutine struct, then allocates memory in the relation's index context (rd_indexcxt) and copies the routine information there. The function is designed to be paranoid about memory leaks by performing the initial call in a short-lived memory context before transferring the data to the persistent index context.

## Parameters / Member Variables
- `relation`: The index relation for which to initialize the access method routine. The relation's rd_amhandler and rd_indexcxt must already be valid.

## Dependencies
- Functions called/Symbols referenced:
  - [GetIndexAmRoutine](../G/GetIndexAmRoutine.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - memcpy
  - [pfree](../p/pfree.md)
  - [IndexAmRoutine](IndexAmRoutine.md) (struct type)
- Called from:
  - [RelationInitIndexAccessInfo](../R/RelationInitIndexAccessInfo.md)
  - [load_relcache_init_file](../l/load_relcache_init_file.md)

## Notes and Other Information
- This is a static function within relcache.c, used internally for relation cache initialization
- The function assumes that relation->rd_amhandler and relation->rd_indexcxt are already valid
- Memory allocation is done in the relation's index context to ensure proper lifecycle management
- The temporary IndexAmRoutine obtained from GetIndexAmRoutine is freed after copying to prevent leaks
- Part of PostgreSQL's relation cache infrastructure for index access method management