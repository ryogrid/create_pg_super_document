# OpclassIsVisibleExt

## Location
[src/backend/catalog/namespace.c:2166-2222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2166-L2222)

## Overview
Extended version of OpclassIsVisible that provides optional error handling for missing operator classes while determining visibility in the current search path.

## Definition

```c
static bool
OpclassIsVisibleExt(Oid opcid, bool *is_missing)
```
## Detailed Description
OpclassIsVisibleExt is the core implementation function for operator class visibility checking in PostgreSQL's namespace system. It determines whether an operator class is visible in the current search path by performing a comprehensive check that includes cache lookup, namespace path verification, and name resolution conflicts.

The function first retrieves the operator class information from the system catalog cache, then checks if its namespace is in the current search path. For operator classes in accessible namespaces, it performs an additional check to ensure the operator class would actually be found by name (not shadowed by another operator class with the same name earlier in the path).

The extended version provides graceful error handling for cases where the operator class doesn't exist, allowing callers to distinguish between "not visible" and "not found" scenarios.

## Parameters / Member Variables
- `opcid`: The OID (Object Identifier) of the operator class to check for visibility
- `is_missing`: Optional pointer to bool; if not NULL and the operator class is not found, sets *is_missing = true and returns false instead of throwing an error (caller must initialize to false)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (CLAOID cache lookup)
  - HeapTupleIsValid
  - GETSTRUCT
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [OpclassnameGetOpcid](OpclassnameGetOpcid.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_opclass
- Called from (representative examples):
  - [OpclassIsVisible](OpclassIsVisible.md) (src/backend/catalog/namespace.c:2156)
  - [pg_opclass_is_visible](../p/pg_opclass_is_visible.md) (src/backend/catalog/namespace.c:4956)

## Notes and Other Information
- This is a static function, not directly accessible outside namespace.c
- Located in src/backend/catalog/namespace.c:2166-2222
- Implements the core logic for PostgreSQL's operator class visibility system
- Handles both system catalog (PG_CATALOG_NAMESPACE) and user-defined namespaces
- Performs name shadowing detection to ensure correct visibility semantics
- Uses the activeSearchPath global variable for namespace resolution
- The is_missing parameter allows callers to differentiate between missing operator classes and invisible ones

## Simplified Source

```c
static bool OpclassIsVisibleExt(Oid opcid, bool *is_missing) {
    HeapTuple opctup;
    Form_pg_opclass opcform;
    Oid opcnamespace;
    bool visible;

    // Look up operator class in system cache
    opctup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opcid));
    if (!HeapTupleIsValid(opctup)) {
        // Handle missing operator class gracefully if requested
        if (is_missing != NULL) {
            *is_missing = true;
            return false;
        }
        elog(ERROR, "cache lookup failed for opclass %u", opcid);
    }

    opcform = (Form_pg_opclass) GETSTRUCT(opctup);
    recomputeNamespacePath();

    // Quick check: if namespace not in search path, not visible
    opcnamespace = opcform->opcnamespace;
    if (opcnamespace != PG_CATALOG_NAMESPACE &&
        !list_member_oid(activeSearchPath, opcnamespace)) {
        visible = false;
    } else {
        // Check if this opclass would be found by name resolution
        char *opcname = NameStr(opcform->opcname);

        // Use the standard name lookup to see if it finds this opclass
        visible = (OpclassnameGetOpcid(opcform->opcmethod, opcname) == opcid);
    }

    ReleaseSysCache(opctup);
    return visible;
}
```