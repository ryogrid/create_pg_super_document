# fmgr_internal_validator

## Location
[src/backend/catalog/pg_proc.c:725-767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_proc.c#L725-L767)

## Overview
Validates that an internal function name refers to a known built-in function in PostgreSQL's function manager.

## Definition


## Detailed Description
This function serves as the validator for internal language functions in PostgreSQL. When a function is created with language 'internal', this validator is called to ensure that the function name (stored in prosrc) corresponds to an actual built-in function that PostgreSQL knows about.

The validator performs a critical security check by verifying that the specified internal function name exists in PostgreSQL's built-in function registry. This prevents creation of functions that reference non-existent internal functions, which could lead to runtime errors or security vulnerabilities.

Unlike other validators, this function does not honor the check_function_bodies GUC setting because internal function names are expected to be resolvable immediately - if a built-in function doesn't exist now, it's unlikely to exist later.

## Parameters / Member Variables
- Takes a single OID parameter via PG_FUNCTION_ARGS:
  - : OID of the function being validated

## Dependencies
- Functions called/Symbols referenced:
  - [CheckFunctionValidatorAccess](../C/CheckFunctionValidatorAccess.md): Verifies permission to validate this function
  - [SearchSysCache1](../S/SearchSysCache1.md): Looks up the function tuple in pg_proc
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md): Gets the prosrc (function source) attribute
  - TextDatumGetCString: Converts the function name from text to C string
  - [fmgr_internal_function](fmgr_internal_function.md): Checks if the function name is a known built-in
  - PG_RETURN_VOID: Returns void datum

- Called from (representative examples):
  - No direct references found in the codebase - typically registered as the validator for 'internal' language

## Notes and Other Information
- This validator is specifically designed for the 'internal' procedural language
- It does not respect the check_function_bodies GUC setting, always performing validation
- The function performs immediate validation since built-in function availability is deterministic
- Failure to find a matching built-in function results in an ERROR with code ERRCODE_UNDEFINED_FUNCTION
- Used internally by PostgreSQL's function creation process when language is set to 'internal'
- Essential for maintaining system security by preventing invalid internal function references