# fetch_finfo_record

## Location
src/backend/utils/fmgr/fmgr.c: 455 - 514

## Overview
This function fetches and validates the information record for a given external C function by looking up its associated PG_FUNCTION_INFO_V1 info function.

## Definition


## Detailed Description
fetch_finfo_record retrieves metadata about a C-language function by:

1. Constructing the info function name by prefixing "pg_finfo_" to the function name
2. Looking up this info function in the loaded shared library using lookup_external_function
3. Calling the info function to get the Pg_finfo_record structure
4. Validating the returned record, checking for null pointer and supported API version
5. Returning the validated info record for use by the function manager

The function ensures that C-language functions have proper PG_FUNCTION_INFO_V1 declarations and provides clear error messages when they're missing or invalid. It currently only supports API version 1.

## Parameters / Member Variables
- : Handle to the loaded shared library containing the function
- : Name of the C function whose info record should be fetched

## Dependencies
- Functions called/Symbols referenced:
  - psprintf (format string into allocated memory)
  - lookup_external_function (find function in shared library)
  - ereport (detailed error reporting)
  - elog (simple error logging)
  - pfree (free allocated memory)
- Called from (representative examples):
  - fmgr_info_C_lang (during function info setup)
  - fmgr_c_validator (during function validation)

## Notes and Other Information
- This function enforces PostgreSQL's requirement that C functions have PG_FUNCTION_INFO_V1 declarations
- It provides helpful error messages suggesting the use of PG_FUNCTION_INFO_V1 macro when info functions are missing
- The function is separated from fmgr_info_C_lang to allow validation of functions not yet in pg_proc
- Memory management includes cleaning up the constructed info function name
- Only API version 1 is currently supported, with clear error messages for unsupported versions