# SearchSysCache3

## Location
src/backend/utils/cache/syscache.c: 243 - 253

## Overview
SearchSysCache3 is a high-level interface function that searches PostgreSQL's system cache for a tuple using three key values.

## Definition


## Detailed Description
SearchSysCache3 provides a convenient wrapper around the lower-level SearchCatCache3 function for searching system caches with exactly three key values. It validates that the cache exists and has the expected number of keys (3) before delegating to the catalog cache search mechanism. The function is part of PostgreSQL's system cache infrastructure that provides fast access to frequently accessed system catalog information.

## Parameters / Member Variables
- : Integer identifier of the system cache to search in
- : First search key value as a Datum
- : Second search key value as a Datum  
- : Third search key value as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - [SearchCatCache3](SearchCatCache3.md)
- Called from (representative examples):
  - [SetDefaultACL](SetDefaultACL.md)
  - [get_default_acl_internal](../g/get_default_acl_internal.md)
  - [lookup_collation](../l/lookup_collation.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [ResolveOpClass](../R/ResolveOpClass.md)

## Notes and Other Information
- The function includes assertions to validate the cache ID is within bounds and that the specified cache exists
- It specifically validates that the cache is configured for exactly 3 keys before proceeding
- Returns a HeapTuple if found, or NULL if no matching tuple exists in the cache
- The returned tuple should be released using ReleaseSysCache when no longer needed