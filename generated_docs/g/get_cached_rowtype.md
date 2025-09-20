# get_cached_rowtype

## Location
[src/backend/executor/execExprInterp.c:2084-2149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2084-L2149)

## Overview
get_cached_rowtype is a utility function that efficiently looks up and caches rowtype tuple descriptors, handling both named composite types and RECORD types with appropriate caching strategies.

## Definition

```c
static TupleDesc
get_cached_rowtype(Oid type_id, int32 typmod,
				   ExprEvalRowtypeCache *rowcache,
				   bool *changed)
```
## Detailed Description
This function provides an optimized way to retrieve TupleDesc information for composite types while maintaining a cache to avoid repeated lookups. It handles two distinct cases:

1. **Named composite types (non-RECORDOID)**: Uses the type cache system and checks for type definition changes using tupDesc_identifier
2. **RECORD types**: Uses direct lookup since RECORD types don't change during backend lifetime

The function is designed to be called multiple times during expression evaluation as composite type definitions can change. It maintains cache consistency by comparing identifiers and invalidating when necessary.

## Parameters / Member Variables
- : OID identifying the rowtype to look up
- : Type modifier for the rowtype (additional type information)
- : Pointer to ExprEvalRowtypeCache structure for caching (cacheptr must be initialized to NULL)
- : Optional pointer to bool that gets set to true when cache is updated

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - TYPECACHE_TUPDESC
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - ReleaseTupleDesc
  - ExprEvalRowtypeCache (structure access)
- Called from (representative examples):
  - EEO_JUMP (macro expansion)
  - [ExecEvalRowNullInt](../E/ExecEvalRowNullInt.md)
  - [ExecEvalFieldSelect](../E/ExecEvalFieldSelect.md)
  - [ExecEvalFieldStoreDeForm](../E/ExecEvalFieldStoreDeForm.md)
  - [ExecEvalFieldStoreForm](../E/ExecEvalFieldStoreForm.md)
  - [ExecEvalConvertRowtype](../E/ExecEvalConvertRowtype.md)

## Notes and Other Information
- Located in src/backend/executor/execExprInterp.c:2084-2149
- The returned TupleDesc is not guaranteed pinned; caller must pin it for operations that might trigger cache invalidation
- [TupleDesc](../T/TupleDesc.md) is always refcounted, so use IncrTupleDescRefCount for pinning
- Must handle the possibility of composite type content changes during execution
- Cannot be called just once during initialization due to potential type changes
- Uses unlikely() optimization hints for cache miss scenarios