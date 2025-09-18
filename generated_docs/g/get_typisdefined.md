# get_typisdefined

## Location
[src/backend/utils/cache/lsyscache.c:2173-2196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2173-L2196)

## Overview
Determines whether a given PostgreSQL data type is fully defined or just a shell type placeholder.

## Definition
```c
bool get_typisdefined(Oid typid)
```

## Detailed Description
This function checks whether a PostgreSQL data type is fully defined by examining the typisdefined field in the pg_type system catalog. In PostgreSQL's type system, types can exist in two states: as fully defined types with complete implementation, or as "shell" types that are placeholder entries created during the type definition process. Shell types are created first to allow for forward references, then later filled in with the complete type definition.

The function returns true if the type is fully defined and ready for use, or false if it's only a shell type or if the type doesn't exist.

## Parameters / Member Variables
- `typid`: The OID (Object Identifier) of the type to check

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract structure from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_type (pg_type tuple structure)
- Called from (representative examples):
  - [RenameTypeInternal](../R/RenameTypeInternal.md)
  - [moveArrayTypeName](../m/moveArrayTypeName.md)
  - [DefineType](../D/DefineType.md)
  - [DefineRange](../D/DefineRange.md)

## Notes and Other Information
- Part of PostgreSQL's type system infrastructure
- Returns false if the type OID doesn't exist in the catalog
- Shell types are temporary placeholders used during type creation to handle circular dependencies
- The typisdefined field is a boolean flag in pg_type that indicates completion of type definition
- Used primarily during DDL operations like CREATE TYPE to ensure type consistency
- Essential for preventing use of incomplete type definitions that could cause system instability
- Part of the low-level system cache API (lsyscache.c) that provides convenient access to catalog information