# Pg_finfo_record

## Location
src/include/fmgr.h: 398 - 414

## Overview
A structure that contains metadata about PostgreSQL extension functions, specifically version information for the function manager's calling convention.

## Definition


## Detailed Description
The `Pg_finfo_record` structure serves as a version information record for PostgreSQL extension functions. It is part of the function manager (fmgr) system that handles dynamic loading and execution of C functions in PostgreSQL extensions. This structure ensures compatibility between different versions of the PostgreSQL function calling convention by storing the API version number that the function was compiled against.

The structure is designed to be extensible - additional fields can be added in future versions while maintaining backward compatibility. Currently, it only contains the `api_version` field, which is set to 1 for the current calling convention.

This record is typically created automatically by the `PG_FUNCTION_INFO_V1` macro, which extension developers use to declare version 1 functions. The function manager uses this information to validate that loaded functions are compatible with the current PostgreSQL version.

## Parameters / Member Variables
- `api_version`: An integer specifying the function calling convention version number. Currently set to 1 for all version 1 functions.

## Dependencies
- Functions called/Symbols referenced:
  - Used by `PGFInfoFunction` typedef
- Called from (representative examples):
  - [fetch_finfo_record](../f/fetch_finfo_record.md) at src/backend/utils/fmgr/fmgr.c:454
  - [fmgr_info_C_lang](../f/fmgr_info_C_lang.md) at src/backend/utils/fmgr/fmgr.c:353
  - [record_C_func](../r/record_C_func.md) at src/backend/utils/fmgr/fmgr.c:540
  - `PG_FUNCTION_INFO_V1` macro at src/include/fmgr.h:417

## Notes and Other Information
- The structure is defined in src/include/fmgr.h:394-398
- Extension authors typically don't interact with this structure directly, but instead use the `PG_FUNCTION_INFO_V1` macro
- The design allows for future extension of the function information system
- Each C function in an extension has an associated info function that returns a pointer to this structure
- The function manager uses this record during dynamic loading to ensure version compatibility