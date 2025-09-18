# TypeIsVisibleExt

## Location
src/backend/catalog/namespace.c: 1052 - 1191

## Overview
Determines whether a type (identified by OID) is visible in the current search path, with optional error handling for missing types.

## Definition


## Detailed Description
TypeIsVisibleExt is an internal function that checks if a specific type is visible in the current namespace search path. It extends the basic visibility checking by providing optional error handling - instead of throwing an error when a type is not found, it can optionally set a flag and return false. The function performs a comprehensive visibility check that considers both namespace membership and potential name conflicts with other types of the same name in earlier namespaces.

The visibility check involves two phases: first, a quick check to see if the type's namespace is in the active search path, and second, a thorough search to ensure the type isn't masked by another type with the same name in an earlier namespace.

## Parameters / Member Variables
- : The OID of the type to check for visibility
- : Optional pointer to a boolean flag; if not NULL, will be set to true if the type is missing instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_type
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - recomputeNamespacePath
  - list_member_oid
  - SearchSysCacheExists2
  - ReleaseSysCache
- Called from (representative examples):
  - TypeIsVisible
  - pg_type_is_visible

## Notes and Other Information
- This is a static function only accessible within namespace.c
- Uses the system cache (TYPEOID) for efficient type lookup
- Handles both system catalog types (PG_CATALOG_NAMESPACE) and user-defined types
- The function is used as the implementation backend for both TypeIsVisible and pg_type_is_visible SQL functions
- Error handling behavior depends on the is_missing parameter - if NULL, throws elog(ERROR) on missing types