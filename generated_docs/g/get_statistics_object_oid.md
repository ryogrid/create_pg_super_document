# get_statistics_object_oid

## Location
[src/backend/catalog/namespace.c:2575-2631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2575-L2631)

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
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - GetSysCacheOid2 (STATEXTNAMENSP cache)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [NameListToString](../N/NameListToString.md)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (in objectaddress.c)
  - [AlterStatistics](../A/AlterStatistics.md) (in statscmds.c)
  - Referenced in namespace.h header

## Notes and Other Information
- Uses the STATEXTNAMENSP system cache for efficient lookups of statistics objects
- Skips temporary namespaces when performing search path resolution
- Part of PostgreSQL's extended statistics system introduced for multi-column statistics
- Follows standard PostgreSQL patterns for qualified name resolution
- Returns InvalidOid when object is not found and missing_ok is true
- Located in src/backend/catalog/namespace.c:2575-2631

## Simplified Source

```c
Oid
get_statistics_object_oid(List *names, bool missing_ok) {
    char *schemaname;
    char *stats_name;
    Oid namespaceId;
    Oid stats_oid = InvalidOid;
    ListCell *l;

    // Parse the qualified name
    DeconstructQualifiedName(names, &schemaname, &stats_name);

    if (schemaname) {
        // Schema-qualified lookup
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if (missing_ok && !OidIsValid(namespaceId))
            stats_oid = InvalidOid;
        else
            stats_oid = GetSysCacheOid2(STATEXTNAMENSP, Anum_pg_statistic_ext_oid,
                                        PointerGetDatum(stats_name),
                                        ObjectIdGetDatum(namespaceId));
    } else {
        // Search through the namespace search path
        recomputeNamespacePath();

        foreach(l, activeSearchPath) {
            namespaceId = lfirst_oid(l);

            // Skip temporary namespace
            if (namespaceId == myTempNamespace)
                continue;

            stats_oid = GetSysCacheOid2(STATEXTNAMENSP, Anum_pg_statistic_ext_oid,
                                        PointerGetDatum(stats_name),
                                        ObjectIdGetDatum(namespaceId));
            if (OidIsValid(stats_oid))
                break;
        }
    }

    // Handle not found case
    if (!OidIsValid(stats_oid) && !missing_ok)
        ereport(ERROR, "statistics object does not exist");

    return stats_oid;
}
```