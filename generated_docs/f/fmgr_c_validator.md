# fmgr_c_validator

## Location
src/backend/catalog/pg_proc.c: 768 - 810

## Overview
Validates C language functions by verifying that the shared library exists, is loadable, and contains the specified function symbol with valid function information.

## Definition


## Detailed Description
This function serves as the validator for C language functions in PostgreSQL. When a function is created with language 'C', this validator is called to ensure that the shared library specified in probin exists, can be loaded, and contains the function symbol specified in prosrc.

The validator performs several critical checks:
1. Loads the shared library specified in the probin field
2. Verifies that the function symbol exists in the loaded library
3. Checks for a valid function information record for the symbol

Unlike the check_function_bodies GUC behavior in other validators, this validator intentionally performs validation even when check_function_bodies is disabled. This is because it's particularly useful during pg_dump restore operations to catch library loading issues early rather than at runtime.

The validation process helps catch common issues like missing shared libraries, incorrect function names, or ABI incompatibilities before the function is actually called.

## Parameters / Member Variables
- Takes a single OID parameter via PG_FUNCTION_ARGS:
  - : OID of the C language function being validated

## Dependencies
- Functions called/Symbols referenced:
  - CheckFunctionValidatorAccess: Verifies permission to validate this function
  - SearchSysCache1: Looks up the function tuple in pg_proc
  - SysCacheGetAttrNotNull: Gets prosrc and probin attributes from the function tuple
  - TextDatumGetCString: Converts text attributes to C strings
  - load_external_function: Loads the shared library and finds the function symbol
  - fetch_finfo_record: Retrieves and validates the function information record
  - PG_RETURN_VOID: Returns void datum

- Called from (representative examples):
  - No direct references found in the codebase - typically registered as the validator for 'C' language

## Notes and Other Information
- This validator is specifically designed for the 'C' procedural language
- Intentionally ignores the check_function_bodies GUC setting to help with pg_dump scenarios
- The validation includes both library loading and symbol resolution checks
- Function information record validation ensures proper ABI compatibility
- Helps detect missing dependencies or incorrect library paths at function creation time
- Essential for preventing runtime errors when C functions are eventually called
- The validator does not cache loaded libraries - each validation loads and unloads the library
- Used internally by PostgreSQL's function creation process when language is set to 'C'