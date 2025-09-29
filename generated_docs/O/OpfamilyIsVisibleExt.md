# OpfamilyIsVisibleExt

## Location
[src/backend/catalog/namespace.c:2268-2321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2268-L2321)

## Overview
Extended version of OpfamilyIsVisible that provides optional error handling for missing operator families while determining visibility in the current search path.

## Definition

```c
static bool
OpfamilyIsVisibleExt(Oid opfid, bool *is_missing)
```
## Detailed Description
OpfamilyIsVisibleExt is the core implementation function for operator family visibility checking in PostgreSQL's namespace system. It determines whether an operator family is visible in the current search path by performing a comprehensive check that includes cache lookup, namespace path verification, and name resolution conflicts.

The function first retrieves the operator family information from the system catalog cache (OPFAMILYOID), then checks if its namespace is in the current search path. For operator families in accessible namespaces, it performs an additional check to ensure the operator family would actually be found by name (not shadowed by another operator family with the same name and access method earlier in the path).

The extended version provides graceful error handling for cases where the operator family doesn't exist, allowing callers to distinguish between "not visible" and "not found" scenarios. The visibility check takes into account both the operator family name and its associated access method.

## Parameters / Member Variables
- `opfid`: The OID (Object Identifier) of the operator family to check for visibility
- `is_missing`: Optional pointer to bool; if not NULL and the operator family is not found, sets *is_missing = true and returns false instead of throwing an error (caller must initialize to false)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (OPFAMILYOID cache lookup)
  - HeapTupleIsValid
  - GETSTRUCT
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [OpfamilynameGetOpfid](OpfamilynameGetOpfid.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_opfamily
- Called from (representative examples):
  - [OpfamilyIsVisible](OpfamilyIsVisible.md) (src/backend/catalog/namespace.c:2258)
  - [pg_opfamily_is_visible](../p/pg_opfamily_is_visible.md) (src/backend/catalog/namespace.c:4970)

## Notes and Other Information
- This is a static function, not directly accessible outside namespace.c
- Located in src/backend/catalog/namespace.c:2268-2321
- Implements the core logic for PostgreSQL's operator family visibility system
- Handles both system catalog (PG_CATALOG_NAMESPACE) and user-defined namespaces
- Performs name shadowing detection considering both name and access method
- Uses the activeSearchPath global variable for namespace resolution
- The is_missing parameter allows callers to differentiate between missing operator families and invisible ones
- Similar in structure to OpclassIsVisibleExt but operates on operator families and includes access method checking

## Simplified Source

```c
static bool OpfamilyIsVisibleExt(Oid opfid, bool *is_missing) {
    HeapTuple opftup;
    Form_pg_opfamily opfform;
    bool visible;

    // Look up operator family in system cache
    opftup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));
    if (!HeapTupleIsValid(opftup)) {
        if (is_missing != NULL) {
            *is_missing = true;
            return false;
        }
        elog(ERROR, "cache lookup failed for opfamily %u", opfid);
    }

    opfform = (Form_pg_opfamily) GETSTRUCT(opftup);
    recomputeNamespacePath();

    // Quick check: if namespace not in search path, not visible
    Oid opfnamespace = opfform->opfnamespace;
    if (opfnamespace != PG_CATALOG_NAMESPACE &&
        !list_member_oid(activeSearchPath, opfnamespace)) {
        visible = false;
    } else {
        // Check if this operator family would be found by name resolution
        char *opfname = NameStr(opfform->opfname);
        visible = (OpfamilynameGetOpfid(opfform->opfmethod, opfname) == opfid);
    }

    ReleaseSysCache(opftup);
    return visible;
}
```