# TSDictionaryIsVisibleExt

## Location
[src/backend/catalog/namespace.c:2931-3006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2931-L3006)

## Overview
TSDictionaryIsVisibleExt determines whether a text search dictionary is visible in the current search path, with optional error handling for missing dictionaries.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(names, &schemaname, &template_name);
```
## Detailed Description
This function checks whether a text search dictionary identified by its OID is visible according to PostgreSQL's namespace search path rules. It extends the basic visibility check by providing graceful error handling when a dictionary is not found in the system catalog. The function performs a comprehensive visibility check that considers:

1. Whether the dictionary exists in the system catalog
2. Whether its namespace is in the current search path
3. Whether it's shadowed by another dictionary with the same name in an earlier namespace

The visibility determination follows PostgreSQL's standard namespace resolution rules, where objects in earlier namespaces in the search path take precedence over those in later namespaces.

## Parameters / Member Variables
- : The OID of the text search dictionary to check for visibility
- : Optional pointer to a boolean flag; if provided and the dictionary is not found, this will be set to true instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (to look up dictionary in pg_ts_dict)
  - Form_pg_ts_dict (struct type for dictionary catalog entries)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md) (to ensure current search path)
  - [list_member_oid](../l/list_member_oid.md) (to check if namespace is in search path)
  - SearchSysCacheExists2 (to check for name conflicts)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (to clean up cache reference)
- Called from (representative examples):
  - [TSDictionaryIsVisible](TSDictionaryIsVisible.md) (wrapper function)
  - [pg_ts_dict_is_visible](../p/pg_ts_dict_is_visible.md) (SQL-callable function)

## Notes and Other Information
- This is a static function, only accessible within namespace.c
- Provides graceful error handling through the is_missing parameter, allowing callers to handle missing dictionaries without exceptions
- Implements PostgreSQL's standard namespace visibility rules for text search objects
- Temporary namespaces are explicitly skipped during visibility checking
- The function follows the pattern of other *IsVisibleExt functions in the codebase for consistent error handling

## Simplified Source

```c
static bool TSDictionaryIsVisibleExt(Oid dictId, bool *is_missing) {
    HeapTuple tuple;
    Form_pg_ts_dict form;
    Oid namespace;
    bool visible;

    // Look up dictionary in system cache
    tuple = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictId));
    if (!HeapTupleIsValid(tuple)) {
        // Handle missing dictionary gracefully if requested
        if (is_missing != NULL) {
            *is_missing = true;
            return false;
        }
        elog(ERROR, "cache lookup failed for text search dictionary %u", dictId);
    }

    form = (Form_pg_ts_dict) GETSTRUCT(tuple);
    recomputeNamespacePath();

    // Quick check: if namespace not in search path, not visible
    namespace = form->dictnamespace;
    if (namespace != PG_CATALOG_NAMESPACE &&
        !list_member_oid(activeSearchPath, namespace)) {
        visible = false;
    } else {
        // Check for name conflicts in earlier namespaces
        char *name = NameStr(form->dictname);
        visible = false;

        // Search through path to see if we find this dictionary first
        foreach(ListCell *l, activeSearchPath) {
            Oid namespaceId = lfirst_oid(l);

            if (namespaceId == myTempNamespace)
                continue;  // Skip temp namespace

            if (namespaceId == namespace) {
                visible = true;  // Found our dictionary first
                break;
            }

            // Check if another dictionary with same name exists here
            if (SearchSysCacheExists2(TSDICTNAMENSP,
                                    PointerGetDatum(name),
                                    ObjectIdGetDatum(namespaceId))) {
                break;  // Found conflicting dictionary earlier in path
            }
        }
    }

    ReleaseSysCache(tuple);
    return visible;
}
```