# get_statistics_object_oid

## Location
src/backend/catalog/namespace.c: 2575 - 2631

## Overview
Finds a statistics object by its possibly qualified name and returns its OID, with support for both schema-qualified and unqualified names.

## Definition
```c
Oid get_statistics_object_oid(List *names, bool missing_ok)
```

## Detailed Description
get_statistics_object_oid is responsible for resolving statistics object names to their corresponding OIDs in PostgreSQL's catalog system. It handles both qualified names (schema.stats_name) and unqualified names that are resolved using the current search path.

For qualified names, it looks up the specified schema and searches for the statistics object within that namespace. For unqualified names, it searches through the active search path namespaces in order until it finds a matching statistics object, excluding temporary namespaces.

The function provides flexible error handling through the missing_ok parameter - it can either return InvalidOid for missing objects or throw a descriptive error.

## Parameters / Member Variables
- `names`: List of name components (either ["stats_name"] or ["schema", "stats_name"])
- `missing_ok`: If true, returns InvalidOid when object not found; if false, throws error

## Dependencies
- Functions called/Symbols referenced:
  - DeconstructQualifiedName
  - LookupExplicitNamespace
  - GetSysCacheOid2 (STATEXTNAMENSP cache)
  - recomputeNamespacePath
  - NameListToString
- Called from (representative examples):
  - get_object_address (in objectaddress.c)
  - AlterStatistics (in statscmds.c)
  - Referenced in namespace.h header

## Notes and Other Information
- Uses the STATEXTNAMENSP system cache for efficient lookups of statistics objects
- Skips temporary namespaces when performing search path resolution
- Part of PostgreSQL's extended statistics system introduced for multi-column statistics
- Follows standard PostgreSQL patterns for qualified name resolution
- Returns InvalidOid when object is not found and missing_ok is true
- Located in src/backend/catalog/namespace.c:2575-2631