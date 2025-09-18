# CollationIsVisible

## Location
src/backend/catalog/namespace.c: 2407 - 2418

## Overview
A simple wrapper function that determines whether a collation identified by OID is visible in the current search path, meaning it would be found when searching for the unqualified collation name.

## Definition
```c
bool CollationIsVisible(Oid collid)
```

## Detailed Description
This function serves as a simplified interface to CollationIsVisibleExt, providing the common use case where only visibility determination is needed without additional output parameters. It checks whether a given collation OID would be found by PostgreSQL's standard namespace resolution algorithm when searching for an unqualified collation name.

The function is part of PostgreSQL's visibility checking infrastructure, which is used throughout the system to determine whether database objects should be displayed to users or are accessible through unqualified names.

## Parameters / Member Variables
- `collid`: The OID of the collation to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [CollationIsVisibleExt](CollationIsVisibleExt.md) (the extended version that does the actual work)
- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md) (for object description formatting)
  - [regcollationout](../r/regcollationout.md) (for collation output formatting)
  - generate_collation_name (for SQL generation)
  - RangeVarGetRelid (via header inclusion)

## Notes and Other Information
- This is a public function exported in namespace.h
- Acts as a convenience wrapper around CollationIsVisibleExt
- Only considers collations that work with the current database encoding as visible
- Returns true if the collation would be found by unqualified name resolution, false otherwise
- Part of PostgreSQL's general object visibility framework used by various system catalogs and utilities