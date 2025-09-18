# SysCacheGetAttr

## Location
[src/backend/utils/cache/syscache.c:601-631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L601-L631)

## Overview
Extracts a specific attribute from a tuple previously fetched by SearchSysCache(), providing type-safe access to system catalog attributes.

## Definition
```c
Datum SysCacheGetAttr(int cacheId, HeapTuple tup, AttrNumber attributeNumber, bool *isNull)
```

## Detailed Description
This function is equivalent to using heap_getattr() on a tuple fetched from a non-cached relation, but specifically designed for system cache tuples. It extracts a specific attribute from a tuple that was previously obtained via SearchSysCache() or related functions. The function handles the necessary tuple descriptor lookup from the cache metadata and applies proper attribute extraction logic.

Usually, this function is only used for attributes that could be NULL or variable length, since fixed-size attributes in system tables are often accessed directly by mapping the tuple onto C struct declarations from include/catalog/. The function properly handles pass-by-reference types by returning pointers into the tuple data area, which the caller must not modify or pfree.

## Parameters / Member Variables
- `cacheId`: The cache ID referencing the system cache (can be different from the original fetch cache)
- `tup`: The HeapTuple previously fetched by SearchSysCache()
- `attributeNumber`: The attribute number to extract from the tuple
- `isNull`: Output parameter set to true if the attribute value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - elog
  - [InitCatCachePhase2](../I/InitCatCachePhase2.md)
  - [heap_getattr](../h/heap_getattr.md)
- Called from (representative examples):
  - [SetDefaultACL](SetDefaultACL.md) (src/backend/catalog/aclchk.c:1290)
  - [ExecGrant_Attribute](../E/ExecGrant_Attribute.md) (src/backend/catalog/aclchk.c:1715)
  - [StorePartitionBound](StorePartitionBound.md) (src/backend/catalog/heap.c:3557)
  - [ProcedureCreate](../P/ProcedureCreate.md) (src/backend/catalog/pg_proc.c:456)
  - [ATExecSetOptions](../A/ATExecSetOptions.md) (src/backend/commands/tablecmds.c:8780)

## Notes and Other Information
- It is legal to use SysCacheGetAttr() with a cacheId referencing a different cache for the same catalog the tuple was fetched from
- The function performs validation on the cacheId parameter and will throw an error for invalid cache IDs
- Initializes cache control data if needed using InitCatCachePhase2
- For pass-by-reference types, returns a pointer into the tuple data area that must not be modified or freed by the caller
- Part of PostgreSQL's system catalog caching infrastructure, widely used throughout the system for accessing catalog metadata
- The tuple descriptor is obtained from the cache entry's cc_tupdesc field