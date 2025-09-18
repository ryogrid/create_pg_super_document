# GetPgIndexDescriptor

## Location
[src/backend/utils/cache/relcache.c:4468-4489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4468-L4489)

## Overview
Returns a cached tuple descriptor for the pg_index system catalog, using lazy initialization to build the descriptor from hardcoded attribute definitions.

## Definition
```c
static TupleDesc GetPgIndexDescriptor(void)
```

## Detailed Description
GetPgIndexDescriptor provides access to a predefined tuple descriptor for the pg_index system catalog. Similar to GetPgClassDescriptor, this function implements lazy initialization, creating the descriptor only when first needed and caching it in a static variable for subsequent calls.

The function uses BuildHardcodedDescriptor with predefined constants (Natts_pg_index and Desc_pg_index) to construct a tuple descriptor that enables access to pg_index fields before the standard catalog cache system is fully operational. This is particularly important for index-related operations during PostgreSQL's bootstrap phase and relcache initialization.

## Parameters
None - this is a parameter-less function.

## Dependencies
- Functions called/Symbols referenced:
  - [BuildHardcodedDescriptor](../B/BuildHardcodedDescriptor.md)
  - Natts_pg_index (constant)
  - Desc_pg_index (constant)
- Called from:
  - OpClassCacheEnt
  - [RelationInitIndexAccessInfo](../R/RelationInitIndexAccessInfo.md)
  - [RelationGetIndexExpressions](../R/RelationGetIndexExpressions.md)
  - [RelationGetDummyIndexExpressions](../R/RelationGetDummyIndexExpressions.md)
  - [RelationGetIndexPredicate](../R/RelationGetIndexPredicate.md)
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)

## Notes and Other Information
- Uses static variable for caching to avoid rebuilding the descriptor on subsequent calls
- Essential for index-related catalog access during PostgreSQL initialization
- Widely used throughout relcache for index information retrieval
- The returned descriptor has the same limitations as BuildHardcodedDescriptor (incorrect rowtype OID, missing TupleConstr)
- Critical for accessing pg_index catalog entries during index access info initialization and expression/predicate extraction