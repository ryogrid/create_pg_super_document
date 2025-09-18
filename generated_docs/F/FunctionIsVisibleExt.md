# FunctionIsVisibleExt

## Location
src/backend/catalog/namespace.c: 1708 - 1784

## Overview
Determines whether a function (identified by OID) is visible in the current search path, with optional error handling for missing functions.

## Definition


## Detailed Description
FunctionIsVisibleExt is the core implementation for function visibility checking in PostgreSQL's namespace system. It determines whether a function would be found when searching for the unqualified function name with exact argument matches. The function performs a comprehensive two-phase visibility check: first verifying the function's namespace is in the search path, then ensuring the function isn't masked by another function with the same name and signature in an earlier namespace.

The 'Ext' suffix indicates this is the extended version that provides optional graceful error handling. When is_missing is provided, the function can return false for missing functions instead of throwing an error, making it suitable for contexts where missing functions should be handled gracefully.

## Parameters / Member Variables
- : The OID of the function to check for visibility
- : Optional pointer to boolean flag; if not NULL, set to true for missing functions instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - recomputeNamespacePath
  - list_member_oid
  - FuncnameGetCandidates
  - makeString
  - ReleaseSysCache
- Called from (representative examples):
  - FunctionIsVisible
  - pg_function_is_visible

## Notes and Other Information
- This is a static function serving as the implementation backend for function visibility checking
- Uses FuncnameGetCandidates to perform the actual function lookup and comparison
- Handles both system catalog functions (PG_CATALOG_NAMESPACE) and user-defined functions
- The visibility check ensures exact argument type matching by comparing proargtypes arrays
- Critical component of PostgreSQL's SQL function visibility system and regproc type operations
- Used by both internal PostgreSQL code and SQL-callable functions like pg_function_is_visible()
- The function correctly handles function overloading by checking both name and argument signatures