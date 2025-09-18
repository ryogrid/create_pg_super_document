# GetPgClassDescriptor

## Location
src/backend/utils/cache/relcache.c: 4455 - 4467

## Overview
Returns a cached tuple descriptor for the pg_class system catalog, using lazy initialization to build the descriptor from hardcoded attribute definitions.

## Definition
```c
static TupleDesc GetPgClassDescriptor(void)
```

## Detailed Description
GetPgClassDescriptor provides access to a predefined tuple descriptor for the pg_class system catalog. This function implements a lazy initialization pattern, creating the descriptor only when first needed and caching it in a static variable for subsequent calls.

The function uses BuildHardcodedDescriptor with predefined constants (Natts_pg_class and Desc_pg_class) to construct a tuple descriptor that can access pg_class fields before the standard catalog cache system is fully initialized. This is essential during PostgreSQL's bootstrap phase when system catalogs must be accessed before the normal caching infrastructure is available.

## Parameters
None - this is a parameter-less function.

## Dependencies
- Functions called/Symbols referenced:
  - [BuildHardcodedDescriptor](../B/BuildHardcodedDescriptor.md)
  - Natts_pg_class (constant)
  - Desc_pg_class (constant)
- Called from:
  - OpClassCacheEnt
  - [RelationParseRelOptions](../R/RelationParseRelOptions.md)

## Notes and Other Information
- Uses static variable for caching to avoid rebuilding the descriptor on subsequent calls
- Part of PostgreSQL's bootstrap mechanism for early catalog access
- The returned descriptor has the same limitations as BuildHardcodedDescriptor (incorrect rowtype OID, missing TupleConstr)
- Essential for accessing pg_class catalog entries during relcache initialization