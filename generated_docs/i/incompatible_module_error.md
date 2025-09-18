# incompatible_module_error

## Location
src/backend/utils/fmgr/dfmgr.c: 306 - 413

## Overview
This function generates detailed error messages when a dynamically loaded PostgreSQL module has incompatible magic block data, providing specific diagnostic information about version and configuration mismatches.

## Definition
```c
static void incompatible_module_error(const char *libname,
                                      const Pg_magic_struct *module_magic_data)
```

## Detailed Description
The `incompatible_module_error` function is responsible for generating comprehensive error reports when a PostgreSQL extension module fails compatibility checks during dynamic loading. The function performs a systematic analysis of the module's magic block data against the server's expected configuration, identifying specific mismatches and providing detailed diagnostic information.

The function first checks for version mismatches, which indicate the module was compiled against a different major version of PostgreSQL. It then checks the ABI extra field for product compatibility. Finally, it examines individual configuration parameters including FUNC_MAX_ARGS, INDEX_MAX_KEYS, NAMEDATALEN, and FLOAT8PASSBYVAL settings, building a detailed error message that helps developers understand exactly what needs to be corrected.

## Parameters / Member Variables
- `libname`: The name/path of the incompatible library file for error reporting
- `module_magic_data`: Pointer to the magic block structure from the loaded module containing its configuration information

## Dependencies
- Functions called/Symbols referenced:
  - Pg_magic_struct (struct type)
  - StringInfoData (for building detailed error messages)
  - initStringInfo
  - appendStringInfo
  - appendStringInfoChar
  - appendStringInfoString
  - ereport
  - errdetail_internal
- Called from:
  - internal_load_library

## Notes and Other Information
- This function is part of PostgreSQL's dynamic function management system located in src/backend/utils/fmgr/dfmgr.c
- Static function - not directly accessible outside of dfmgr.c
- Handles version number formatting differently for versions >= 1000 vs. older versions
- Provides localized error messages using the _() macro for internationalization
- Builds comprehensive error details by checking multiple configuration parameters systematically
- The function always terminates with ereport(ERROR), making it a no-return function in practice
- Critical for preventing crashes and data corruption that could result from loading incompatible modules
- The error checking must be updated whenever new fields are added to the Pg_magic_struct
- Helps developers quickly identify what compilation parameters need to be adjusted when building extensions