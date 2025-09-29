# TSTemplateIsVisibleExt

## Location
[src/backend/catalog/namespace.c:3077-3151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3077-L3151)

## Overview
TSTemplateIsVisibleExt determines whether a text search template is visible in the current search path, with optional error handling for missing templates.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(names, &schemaname, &config_name);
```
## Detailed Description
This function performs a comprehensive visibility check for text search templates identified by their OID. It extends the basic visibility functionality by providing graceful error handling when a template is not found in the system catalog. The function implements PostgreSQL's standard namespace resolution rules to determine whether a template would be found during an unqualified name search.

The visibility determination process includes:
1. Looking up the template in the pg_ts_template system catalog
2. Checking if the template's namespace is in the current search path
3. Verifying that no template with the same name exists in an earlier namespace in the search path
4. Handling missing templates gracefully based on the is_missing parameter

The function follows PostgreSQL's namespace precedence rules where objects in earlier namespaces shadow those with the same name in later namespaces.

## Parameters / Member Variables
- : The OID of the text search template to check for visibility
- : Optional pointer to a boolean flag; if provided and the template is not found, this will be set to true instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (to look up template in pg_ts_template)
  - Form_pg_ts_template (struct type for template catalog entries)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md) (to ensure current search path)
  - [list_member_oid](../l/list_member_oid.md) (to check if namespace is in search path)
  - SearchSysCacheExists2 (to check for name conflicts)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (to clean up cache reference)
- Called from (representative examples):
  - [TSTemplateIsVisible](TSTemplateIsVisible.md) (wrapper function)
  - [pg_ts_template_is_visible](../p/pg_ts_template_is_visible.md) (SQL-callable function)

## Notes and Other Information
- This is a static function, only accessible within namespace.c
- Provides graceful error handling through the is_missing parameter, allowing callers to handle missing templates without exceptions
- Implements PostgreSQL's standard namespace visibility rules for text search objects
- Temporary namespaces are explicitly skipped during visibility checking
- The function follows the pattern of other *IsVisibleExt functions in the codebase for consistent error handling
- Essential for text search template management and proper namespace isolation

## Simplified Source

```c
static bool TSTemplateIsVisibleExt(Oid tmplId, bool *is_missing) {
    HeapTuple tuple;
    Form_pg_ts_template form;
    Oid namespace;
    bool visible;

    // Look up template in system cache
    tuple = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum(tmplId));
    if (!HeapTupleIsValid(tuple)) {
        // Handle missing template gracefully if requested
        if (is_missing != NULL) {
            *is_missing = true;
            return false;
        }
        elog(ERROR, "cache lookup failed for text search template %u", tmplId);
    }

    form = (Form_pg_ts_template) GETSTRUCT(tuple);
    recomputeNamespacePath();

    // Quick check: if namespace not in search path, not visible
    namespace = form->tmplnamespace;
    if (namespace != PG_CATALOG_NAMESPACE &&
        !list_member_oid(activeSearchPath, namespace)) {
        visible = false;
    } else {
        // Check for name conflicts in earlier namespaces
        char *name = NameStr(form->tmplname);
        visible = false;

        // Search through path to see if we find this template first
        foreach(ListCell *l, activeSearchPath) {
            Oid namespaceId = lfirst_oid(l);

            if (namespaceId == myTempNamespace)
                continue;  // Skip temp namespace

            if (namespaceId == namespace) {
                visible = true;  // Found our template first
                break;
            }

            // Check if another template with same name exists here
            if (SearchSysCacheExists2(TSTEMPLATENAMENSP,
                                    PointerGetDatum(name),
                                    ObjectIdGetDatum(namespaceId))) {
                break;  // Found conflicting template earlier in path
            }
        }
    }

    ReleaseSysCache(tuple);
    return visible;
}
```