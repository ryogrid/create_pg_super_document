# TSConfigIsVisibleExt

## Location
[src/backend/catalog/namespace.c:3222-3300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3222-L3300)

## Overview
An extended version of TSConfigIsVisible that determines whether a text search configuration is visible in the current search path, with optional error handling for missing configurations.

## Definition
```c
static bool TSConfigIsVisibleExt(Oid cfgid, bool *is_missing)
```

## Detailed Description
TSConfigIsVisibleExt performs the core logic for determining text search configuration visibility. It looks up the configuration in the system catalog, recomputes the namespace search path, and checks whether the configuration would be found when searching by its unqualified name. The function handles name conflicts by checking if other configurations with the same name appear earlier in the search path. If is_missing is provided, the function can return false for missing configurations instead of throwing an error.

## Parameters / Member Variables
- `cfgid`: The OID of the text search configuration to check for visibility
- `is_missing`: Optional pointer to bool flag; if not NULL and configuration is missing, sets *is_missing = true and returns false instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_member_oid](../l/list_member_oid.md)
  - SearchSysCacheExists2
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [TSConfigIsVisible](TSConfigIsVisible.md)
  - [pg_ts_config_is_visible](../p/pg_ts_config_is_visible.md)

## Notes and Other Information
- This is a static function, only accessible within namespace.c
- Performs a two-phase visibility check: first checks if the namespace is in the search path, then checks for name conflicts
- Configurations in PG_CATALOG_NAMESPACE are always considered to be in the path
- Skips temporary namespace during visibility checks
- The function carefully handles the search path order to determine which configuration would be found first
- Located in src/backend/catalog/namespace.c:3222-3300

## Simplified Source

```c
static bool TSConfigIsVisibleExt(Oid cfgid, bool *is_missing) {
    HeapTuple tuple;
    Form_pg_ts_config form;
    Oid namespace;
    bool visible;

    // Look up the configuration in the system cache
    tuple = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgid));
    if (!HeapTupleIsValid(tuple)) {
        // Handle missing configuration gracefully if requested
        if (is_missing != NULL) {
            *is_missing = true;
            return false;
        }
        elog(ERROR, "cache lookup failed for text search configuration %u", cfgid);
    }

    form = (Form_pg_ts_config) GETSTRUCT(tuple);
    recomputeNamespacePath();

    // Quick check: if namespace not in search path, not visible
    namespace = form->cfgnamespace;
    if (namespace != PG_CATALOG_NAMESPACE &&
        !list_member_oid(activeSearchPath, namespace)) {
        visible = false;
    } else {
        // Check for name conflicts in earlier namespaces
        char *name = NameStr(form->cfgname);
        visible = false;

        // Search through path to see if we find this config first
        foreach(ListCell *l, activeSearchPath) {
            Oid namespaceId = lfirst_oid(l);

            if (namespaceId == myTempNamespace)
                continue;  // Skip temp namespace

            if (namespaceId == namespace) {
                visible = true;  // Found our config first
                break;
            }

            // Check if another config with same name exists here
            if (SearchSysCacheExists2(TSCONFIGNAMENSP,
                                    PointerGetDatum(name),
                                    ObjectIdGetDatum(namespaceId))) {
                break;  // Found conflicting config earlier in path
            }
        }
    }

    ReleaseSysCache(tuple);
    return visible;
}
```