# TSParserIsVisibleExt

## Location
[src/backend/catalog/namespace.c:2786-2860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2786-L2860)

## Overview
Extended version of TSParserIsVisible that determines parser visibility in the search path with optional missing object handling.

## Definition
```c
static bool TSParserIsVisibleExt(Oid prsId, bool *is_missing)
```

## Detailed Description
This function performs the core logic for determining whether a text search parser is visible in the current search path. It implements a two-phase visibility check:

1. **Path membership check**: Quickly determines if the parser's namespace is in the active search path at all. System catalog objects (PG_CATALOG_NAMESPACE) are always considered to be in the path.

2. **Name conflict resolution**: If the namespace is in the path, performs a detailed check to ensure no other parser with the same name appears earlier in the search path, which would shadow this parser.

The function handles missing parsers gracefully when the is_missing parameter is provided, setting it to true and returning false instead of throwing an error.

## Parameters
- `prsId`: The OID of the text search parser to check for visibility
- `is_missing`: Optional pointer to bool; if provided and parser doesn't exist, sets *is_missing = true instead of throwing error (caller must initialize to false)

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_ts_parser (struct type)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_member_oid](../l/list_member_oid.md)
  - SearchSysCacheExists2
- Called from (representative examples):
  - [TSParserIsVisible](TSParserIsVisible.md)
  - [pg_ts_parser_is_visible](../p/pg_ts_parser_is_visible.md)

## Notes and Other Information
- Static function, only accessible within namespace.c
- Uses TSPARSEROID system cache to look up parser details
- Skips temporary namespaces during search path traversal
- System catalog parsers (PG_CATALOG_NAMESPACE) are always considered visible if they exist
- Implements proper namespace shadowing semantics - earlier entries in search path hide later ones with same name
- Part of PostgreSQL's visibility infrastructure for text search objects
- Located in src/backend/catalog/namespace.c at lines 2786-2860

## Simplified Source

```c
static bool TSParserIsVisibleExt(Oid prsId, bool *is_missing) {
    HeapTuple tuple;
    Form_pg_ts_parser form;
    Oid namespace;
    bool visible;

    // Look up parser in system cache
    tuple = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(prsId));
    if (!HeapTupleIsValid(tuple)) {
        // Handle missing parser gracefully if requested
        if (is_missing != NULL) {
            *is_missing = true;
            return false;
        }
        elog(ERROR, "cache lookup failed for text search parser %u", prsId);
    }

    form = (Form_pg_ts_parser) GETSTRUCT(tuple);
    recomputeNamespacePath();

    // Quick check: if namespace not in search path, not visible
    namespace = form->prsnamespace;
    if (namespace != PG_CATALOG_NAMESPACE &&
        !list_member_oid(activeSearchPath, namespace)) {
        visible = false;
    } else {
        // Check for name conflicts in earlier namespaces
        char *name = NameStr(form->prsname);
        visible = false;

        // Search through path to see if we find this parser first
        foreach(ListCell *l, activeSearchPath) {
            Oid namespaceId = lfirst_oid(l);

            if (namespaceId == myTempNamespace)
                continue;  // Skip temp namespace

            if (namespaceId == namespace) {
                visible = true;  // Found our parser first
                break;
            }

            // Check if another parser with same name exists here
            if (SearchSysCacheExists2(TSPARSERNAMENSP,
                                    PointerGetDatum(name),
                                    ObjectIdGetDatum(namespaceId))) {
                break;  // Found conflicting parser earlier in path
            }
        }
    }

    ReleaseSysCache(tuple);
    return visible;
}
```