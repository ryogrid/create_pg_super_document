# OpclassIsVisibleExt

## Location
src/backend/catalog/namespace.c: 2166 - 2222

## Overview
Extended version of OpclassIsVisible that provides optional error handling for missing operator classes while determining visibility in the current search path.

## Definition


## Detailed Description
OpclassIsVisibleExt is the core implementation function for operator class visibility checking in PostgreSQL's namespace system. It determines whether an operator class is visible in the current search path by performing a comprehensive check that includes cache lookup, namespace path verification, and name resolution conflicts.

The function first retrieves the operator class information from the system catalog cache, then checks if its namespace is in the current search path. For operator classes in accessible namespaces, it performs an additional check to ensure the operator class would actually be found by name (not shadowed by another operator class with the same name earlier in the path).

The extended version provides graceful error handling for cases where the operator class doesn't exist, allowing callers to distinguish between "not visible" and "not found" scenarios.

## Parameters / Member Variables
- `opcid`: The OID (Object Identifier) of the operator class to check for visibility
- `is_missing`: Optional pointer to bool; if not NULL and the operator class is not found, sets *is_missing = true and returns false instead of throwing an error (caller must initialize to false)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (CLAOID cache lookup)
  - HeapTupleIsValid
  - GETSTRUCT
  - recomputeNamespacePath
  - list_member_oid
  - OpclassnameGetOpcid
  - ReleaseSysCache
  - Form_pg_opclass
- Called from (representative examples):
  - OpclassIsVisible (src/backend/catalog/namespace.c:2156)
  - pg_opclass_is_visible (src/backend/catalog/namespace.c:4956)

## Notes and Other Information
- This is a static function, not directly accessible outside namespace.c
- Located in src/backend/catalog/namespace.c:2166-2222
- Implements the core logic for PostgreSQL's operator class visibility system
- Handles both system catalog (PG_CATALOG_NAMESPACE) and user-defined namespaces
- Performs name shadowing detection to ensure correct visibility semantics
- Uses the activeSearchPath global variable for namespace resolution
- The is_missing parameter allows callers to differentiate between missing operator classes and invisible ones