# TSDictionaryIsVisibleExt

## Location
src/backend/catalog/namespace.c: 2931 - 3006

## Overview
TSDictionaryIsVisibleExt determines whether a text search dictionary is visible in the current search path, with optional error handling for missing dictionaries.

## Definition


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
  - SearchSysCache1 (to look up dictionary in pg_ts_dict)
  - Form_pg_ts_dict (struct type for dictionary catalog entries)
  - recomputeNamespacePath (to ensure current search path)
  - list_member_oid (to check if namespace is in search path)
  - SearchSysCacheExists2 (to check for name conflicts)
  - ReleaseSysCache (to clean up cache reference)
- Called from (representative examples):
  - TSDictionaryIsVisible (wrapper function)
  - pg_ts_dict_is_visible (SQL-callable function)

## Notes and Other Information
- This is a static function, only accessible within namespace.c
- Provides graceful error handling through the is_missing parameter, allowing callers to handle missing dictionaries without exceptions
- Implements PostgreSQL's standard namespace visibility rules for text search objects
- Temporary namespaces are explicitly skipped during visibility checking
- The function follows the pattern of other *IsVisibleExt functions in the codebase for consistent error handling