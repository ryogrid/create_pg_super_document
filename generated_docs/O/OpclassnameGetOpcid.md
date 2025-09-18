# OpclassnameGetOpcid

## Location
src/backend/catalog/namespace.c: 2121 - 2153

## Overview
OpclassnameGetOpcid resolves an unqualified index operator class name to its OID within the context of a specific access method.

## Definition


## Detailed Description
This function performs operator class resolution by searching through the current namespace search path for an operator class with the specified name that belongs to the given access method. It is conceptually similar to TypenameGetTypid but specialized for operator classes and requiring an additional access method OID parameter. The function searches each namespace in the active search path sequentially and returns the OID of the first matching operator class found, or InvalidOid if no match is found. The temporary namespace is excluded from the search.

## Parameters / Member Variables
- : OID of the access method (index method) that the operator class belongs to
- : Name of the operator class to resolve (unqualified)

## Dependencies
- Functions called/Symbols referenced:
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - GetSysCacheOid3
- Called from (representative examples):
  - [OpclassIsVisibleExt](OpclassIsVisibleExt.md)
  - [ResolveOpClass](../R/ResolveOpClass.md)
  - [OpClassCacheLookup](OpClassCacheLookup.md)

## Notes and Other Information
- Returns InvalidOid if the operator class is not found in any namespace in the search path
- Excludes the temporary namespace from search (myTempNamespace is skipped)
- Uses the CLAAMNAMENSP system cache for efficient lookup by access method, name, and namespace
- Essential for index creation and maintenance operations where operator classes must be resolved
- Part of PostgreSQL's extensible indexing framework supporting multiple access methods