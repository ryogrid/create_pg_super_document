# FunctionIsVisible

## Location
[src/backend/catalog/namespace.c:1696-1707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L1696-L1707)

## Overview
Determines whether a function (identified by OID) is visible in the current search path using standard error handling.

## Definition


## Detailed Description
FunctionIsVisible is a simple wrapper function that provides the standard interface for checking function visibility in PostgreSQL's namespace system. It determines whether a function would be found when searching for the unqualified function name with exact argument matches in the current search path. This function provides standard error handling behavior, throwing an error if the function OID is not found.

The function delegates all actual work to FunctionIsVisibleExt, passing NULL for the is_missing parameter to indicate that standard error handling (throwing exceptions for missing functions) should be used.

## Parameters / Member Variables
- : The OID of the function to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionIsVisibleExt](FunctionIsVisibleExt.md)
- Called from (representative examples):
  - [format_procedure_extended](../f/format_procedure_extended.md)

## Notes and Other Information
- This is the public interface for function visibility checking with standard error handling
- Returns true if the function is visible (would be found in an unqualified search), false otherwise
- Throws an error if the function OID does not exist in pg_proc
- Commonly used throughout PostgreSQL for determining whether functions should be displayed or referenced without schema qualification
- Part of PostgreSQL's broader namespace visibility system for database objects