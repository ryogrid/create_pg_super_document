# FunctionIsVisibleExt

## Location
[src/backend/catalog/namespace.c:1708-1784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L1708-L1784)

## Overview
Determines whether a function (identified by OID) is visible in the current search path, with optional error handling for missing functions.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(names, &schemaname, &opername);
```
## Detailed Description
FunctionIsVisibleExt is the core implementation for function visibility checking in PostgreSQL's namespace system. It determines whether a function would be found when searching for the unqualified function name with exact argument matches. The function performs a comprehensive two-phase visibility check: first verifying the function's namespace is in the search path, then ensuring the function isn't masked by another function with the same name and signature in an earlier namespace.

The 'Ext' suffix indicates this is the extended version that provides optional graceful error handling. When is_missing is provided, the function can return false for missing functions instead of throwing an error, making it suitable for contexts where missing functions should be handled gracefully.

## Parameters / Member Variables
- : The OID of the function to check for visibility
- : Optional pointer to boolean flag; if not NULL, set to true for missing functions instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [FuncnameGetCandidates](FuncnameGetCandidates.md)
  - [makeString](../m/makeString.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [FunctionIsVisible](FunctionIsVisible.md)
  - [pg_function_is_visible](../p/pg_function_is_visible.md)

## Notes and Other Information
- This is a static function serving as the implementation backend for function visibility checking
- Uses FuncnameGetCandidates to perform the actual function lookup and comparison
- Handles both system catalog functions (PG_CATALOG_NAMESPACE) and user-defined functions
- The visibility check ensures exact argument type matching by comparing proargtypes arrays
- Critical component of PostgreSQL's SQL function visibility system and regproc type operations
- Used by both internal PostgreSQL code and SQL-callable functions like pg_function_is_visible()
- The function correctly handles function overloading by checking both name and argument signatures

## Simplified Source

```c
static bool FunctionIsVisibleExt(Oid funcid, bool *is_missing) {
    HeapTuple proctup;
    Form_pg_proc procform;
    bool visible;

    // Look up function in system cache
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup)) {
        if (is_missing != NULL) {
            *is_missing = true;
            return false;
        }
        elog(ERROR, "cache lookup failed for function %u", funcid);
    }

    procform = (Form_pg_proc) GETSTRUCT(proctup);
    recomputeNamespacePath();

    // Quick check: if namespace not in search path, not visible
    Oid pronamespace = procform->pronamespace;
    if (pronamespace != PG_CATALOG_NAMESPACE &&
        !list_member_oid(activeSearchPath, pronamespace)) {
        visible = false;
    } else {
        // Check if this function would be found by name resolution
        char *proname = NameStr(procform->proname);
        int nargs = procform->pronargs;

        visible = false;
        FuncCandidateList clist = FuncnameGetCandidates(
            list_make1(makeString(proname)), nargs, NIL, false, false, false, false);

        // Search through candidates for exact match
        for (; clist; clist = clist->next) {
            if (memcmp(clist->args, procform->proargtypes.values,
                      nargs * sizeof(Oid)) == 0) {
                visible = (clist->oid == funcid);
                break;
            }
        }
    }

    ReleaseSysCache(proctup);
    return visible;
}
```