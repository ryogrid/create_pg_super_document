# ConversionGetConid

## Location
src/backend/catalog/namespace.c: 2477 - 2508

## Overview
A public function that resolves an unqualified conversion name by searching through the database's active search path, returning the OID of the first matching conversion found.

## Definition
```c
Oid ConversionGetConid(const char *conname)
```

## Detailed Description
This function implements the standard PostgreSQL namespace resolution algorithm for encoding conversions. It iterates through the active search path namespaces in order, using the system cache to find a conversion with the given name. The function follows the same pattern as other object resolution functions like RelnameGetRelid, excluding the temporary namespace from the search per PostgreSQL's general namespace resolution rules.

Conversions in PostgreSQL are used to transform text between different character encodings, and this function provides the mechanism to resolve conversion names in SQL statements and other contexts where conversions need to be looked up by name.

## Parameters / Member Variables
- `conname`: The unqualified name of the conversion to resolve

## Dependencies
- Functions called/Symbols referenced:
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md) (to ensure search path is current)
  - GetSysCacheOid2 (to lookup conversion by name and namespace)
- Called from (representative examples):
  - [ConversionIsVisibleExt](ConversionIsVisibleExt.md)
  - RangeVarGetRelid (via header inclusion)

## Notes and Other Information
- This is a public function exported in namespace.h
- Follows standard PostgreSQL search path resolution, excluding temporary namespaces
- Returns InvalidOid if no matching conversion is found in the search path
- The search stops at the first matching conversion found, following namespace precedence order
- Uses the CONNAMENSP system cache for efficient catalog lookups
- Part of PostgreSQL's general object resolution framework for encoding conversions
- The comment notes this is essentially the same implementation pattern as RelnameGetRelid