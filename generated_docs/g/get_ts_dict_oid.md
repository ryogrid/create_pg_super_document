# get_ts_dict_oid

## Location
[src/backend/catalog/namespace.c:2861-2918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2861-L2918)

## Overview
Finds a text search dictionary by its possibly qualified name and returns its OID, with optional error handling for missing dictionaries.

## Definition
```c
Oid get_ts_dict_oid(List *names, bool missing_ok)
```

## Detailed Description
This function resolves a text search dictionary name (which may be schema-qualified) to its object identifier (OID). It follows the same pattern as get_ts_parser_oid but operates on text search dictionaries instead of parsers. The function supports both qualified and unqualified dictionary names:

1. **Schema-qualified names**: When a schema is explicitly specified (e.g., "public.english_stem"), it looks up the dictionary directly in that specific schema using the TSDICTNAMENSP system cache.
2. **Unqualified names**: When no schema is specified (e.g., just "english_stem"), it searches through the active search path to find the first matching dictionary, excluding temporary namespaces.

The function uses efficient system cache lookups to resolve dictionary names to OIDs and provides flexible error handling.

## Parameters
- `names`: A List containing the dictionary name, possibly schema-qualified (e.g., ["pg_catalog", "english_stem"] or just ["english_stem"])
- `missing_ok`: If true, returns InvalidOid when dictionary is not found; if false, throws an error with ERRCODE_UNDEFINED_OBJECT

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - GetSysCacheOid2
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [NameListToString](../N/NameListToString.md)
- Called from (representative examples):
  - [get_object_address](get_object_address.md)
  - [AlterTSDictionary](../A/AlterTSDictionary.md)
  - [MakeConfigurationMapping](../M/MakeConfigurationMapping.md)
  - [thesaurus_init](../t/thesaurus_init.md)
  - [regdictionaryin](../r/regdictionaryin.md)

## Notes and Other Information
- Returns InvalidOid for non-existent dictionaries when missing_ok is true
- Throws ERRCODE_UNDEFINED_OBJECT error when missing_ok is false and dictionary doesn't exist
- Skips temporary namespaces during search path traversal for security and consistency
- Part of PostgreSQL's text search infrastructure, used for dictionary-based text processing
- Mirrors the functionality of get_ts_parser_oid but for dictionary objects
- Located in src/backend/catalog/namespace.c at lines 2861-2918

## Simplified Source

```c
Oid
get_ts_dict_oid(List *names, bool missing_ok) {
    char *schemaname;
    char *dict_name;
    Oid namespaceId;
    Oid dictoid = InvalidOid;
    ListCell *l;

    // Parse the qualified name
    DeconstructQualifiedName(names, &schemaname, &dict_name);

    if (schemaname) {
        // Schema-qualified lookup
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if (missing_ok && !OidIsValid(namespaceId))
            dictoid = InvalidOid;
        else
            dictoid = GetSysCacheOid2(TSDICTNAMENSP, Anum_pg_ts_dict_oid,
                                      PointerGetDatum(dict_name),
                                      ObjectIdGetDatum(namespaceId));
    } else {
        // Search through the namespace search path
        recomputeNamespacePath();

        foreach(l, activeSearchPath) {
            namespaceId = lfirst_oid(l);

            // Skip temporary namespace
            if (namespaceId == myTempNamespace)
                continue;

            dictoid = GetSysCacheOid2(TSDICTNAMENSP, Anum_pg_ts_dict_oid,
                                      PointerGetDatum(dict_name),
                                      ObjectIdGetDatum(namespaceId));
            if (OidIsValid(dictoid))
                break;
        }
    }

    // Handle not found case
    if (!OidIsValid(dictoid) && !missing_ok)
        ereport(ERROR, "text search dictionary does not exist");

    return dictoid;
}
```