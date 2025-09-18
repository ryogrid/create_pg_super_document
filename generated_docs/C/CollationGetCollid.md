# CollationGetCollid

## Location
src/backend/catalog/namespace.c: 2373 - 2406

## Overview
A public function that resolves an unqualified collation name by searching through the database's active search path, returning the OID of the first matching collation that works with the current database encoding.

## Definition
```c
Oid CollationGetCollid(const char *collname)
```

## Detailed Description
This function implements the standard PostgreSQL namespace resolution algorithm for collations. It iterates through the active search path namespaces in order, using the lookup_collation helper function to find a collation with the given name that is compatible with the current database encoding. The function specifically excludes the temporary namespace from the search, following PostgreSQL's general namespace resolution rules.

This is the primary entry point for resolving unqualified collation names in SQL statements and other contexts where collations need to be looked up by name.

## Parameters / Member Variables
- `collname`: The unqualified name of the collation to resolve

## Dependencies
- Functions called/Symbols referenced:
  - GetDatabaseEncoding (to get current database encoding)
  - recomputeNamespacePath (to ensure search path is current)
  - lookup_collation (to perform actual collation lookup in each namespace)
- Called from (representative examples):
  - CollationIsVisibleExt
  - RangeVarGetRelid (via header inclusion)

## Notes and Other Information
- This is a public function exported in namespace.h
- Only finds collations compatible with the current database encoding
- Follows standard PostgreSQL search path resolution, excluding temporary namespaces
- Returns InvalidOid if no matching collation is found in the search path
- The search stops at the first matching collation found, following namespace precedence order
- Part of PostgreSQL's general object resolution framework