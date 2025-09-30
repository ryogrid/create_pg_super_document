# get_ts_parser_oid

## Location
[src/backend/catalog/namespace.c:2716-2773](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2716-L2773)

## Overview
Finds a text search parser by its possibly qualified name and returns its OID, with optional error handling for missing parsers.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(names, &schemaname, &parser_name);
```
## Detailed Description
This function resolves a text search parser name (which may be schema-qualified) to its object identifier (OID). It supports both fully qualified names (schema.parser) and unqualified names that are resolved using the search path. The function handles two main scenarios:

1. **Schema-qualified names**: When a schema is explicitly specified, it looks up the parser directly in that specific schema using the system cache.
2. **Unqualified names**: When no schema is specified, it searches through the active search path to find the first matching parser, excluding temporary namespaces.

The function uses the TSPARSERNAMENSP system cache to efficiently locate parsers by name and namespace combination.

## Parameters
- : A List containing the parser name, possibly schema-qualified (e.g., ["public", "default"] or just ["default"])
- : If true, returns InvalidOid when parser is not found; if false, throws an error

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)  
  - GetSysCacheOid2
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [NameListToString](../N/NameListToString.md)
- Called from (representative examples):
  - [get_object_address](get_object_address.md)
  - [DefineTSConfiguration](../D/DefineTSConfiguration.md)
  - [ts_token_type_byname](../t/ts_token_type_byname.md)
  - [ts_parse_byname](../t/ts_parse_byname.md)

## Notes and Other Information
- Returns InvalidOid for non-existent parsers when missing_ok is true
- Throws ERRCODE_UNDEFINED_OBJECT error when missing_ok is false and parser doesn't exist
- Skips temporary namespaces during search path traversal
- Part of PostgreSQL's text search infrastructure for full-text search functionality
- Located in src/backend/catalog/namespace.c at lines 2716-2773

## Simplified Source

```c
Oid
get_ts_parser_oid(List *names, bool missing_ok) {
    char *schemaname;
    char *parser_name;
    Oid namespaceId;
    Oid prsoid = InvalidOid;
    ListCell *l;

    // Parse the qualified name
    DeconstructQualifiedName(names, &schemaname, &parser_name);

    if (schemaname) {
        // Schema-qualified lookup
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if (missing_ok && !OidIsValid(namespaceId))
            prsoid = InvalidOid;
        else
            prsoid = GetSysCacheOid2(TSPARSERNAMENSP, Anum_pg_ts_parser_oid,
                                     PointerGetDatum(parser_name),
                                     ObjectIdGetDatum(namespaceId));
    } else {
        // Search through the namespace search path
        recomputeNamespacePath();

        foreach(l, activeSearchPath) {
            namespaceId = lfirst_oid(l);

            // Skip temporary namespace
            if (namespaceId == myTempNamespace)
                continue;

            prsoid = GetSysCacheOid2(TSPARSERNAMENSP, Anum_pg_ts_parser_oid,
                                     PointerGetDatum(parser_name),
                                     ObjectIdGetDatum(namespaceId));
            if (OidIsValid(prsoid))
                break;
        }
    }

    // Handle not found case
    if (!OidIsValid(prsoid) && !missing_ok)
        ereport(ERROR, "text search parser does not exist");

    return prsoid;
}
```