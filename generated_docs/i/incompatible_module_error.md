# incompatible_module_error

## Location
[src/backend/utils/fmgr/dfmgr.c:306-413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L306-L413)

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
  - [StringInfoData](../S/StringInfoData.md) (for building detailed error messages)
  - [initStringInfo](initStringInfo.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - ereport
  - [errdetail_internal](../e/errdetail_internal.md)
- Called from:
  - [internal_load_library](internal_load_library.md)

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

## Simplified Source

```c
// Simplified version of incompatible_module_error
static void incompatible_module_error(const char *libname,
                                     const Pg_magic_struct *module_magic_data)
{
    // Check version mismatch first (most critical check)
    if (magic_data.version != module_magic_data->version) {
        char library_version[32];

        // Format version number based on version scheme
        if (module_magic_data->version >= 1000)
            snprintf(library_version, sizeof(library_version), "%d",
                    module_magic_data->version / 100);
        else
            snprintf(library_version, sizeof(library_version), "%d.%d",
                    module_magic_data->version / 100,
                    module_magic_data->version % 100);

        ereport(ERROR,
                (errmsg("incompatible library \"%s\": version mismatch", libname),
                 errdetail("Server is version %d, library is version %s.",
                          magic_data.version / 100, library_version)));
    }

    // Check ABI compatibility (product mismatch)
    if (strcmp(module_magic_data->abi_extra, magic_data.abi_extra) != 0) {
        ereport(ERROR,
                (errmsg("incompatible library \"%s\": ABI mismatch", libname),
                 errdetail("Server has ABI \"%s\", library has \"%s\".",
                          magic_data.abi_extra, module_magic_data->abi_extra)));
    }

    // Build detailed error message for configuration mismatches
    StringInfoData details;
    initStringInfo(&details);

    // Check each configuration parameter and build error details
    if (module_magic_data->funcmaxargs != magic_data.funcmaxargs) {
        appendStringInfo(&details,
                        "Server has FUNC_MAX_ARGS = %d, library has %d.",
                        magic_data.funcmaxargs, module_magic_data->funcmaxargs);
    }

    if (module_magic_data->indexmaxkeys != magic_data.indexmaxkeys) {
        if (details.len) appendStringInfoChar(&details, '\n');
        appendStringInfo(&details,
                        "Server has INDEX_MAX_KEYS = %d, library has %d.",
                        magic_data.indexmaxkeys, module_magic_data->indexmaxkeys);
    }

    if (module_magic_data->namedatalen != magic_data.namedatalen) {
        if (details.len) appendStringInfoChar(&details, '\n');
        appendStringInfo(&details,
                        "Server has NAMEDATALEN = %d, library has %d.",
                        magic_data.namedatalen, module_magic_data->namedatalen);
    }

    if (module_magic_data->float8byval != magic_data.float8byval) {
        if (details.len) appendStringInfoChar(&details, '\n');
        appendStringInfo(&details,
                        "Server has FLOAT8PASSBYVAL = %s, library has %s.",
                        magic_data.float8byval ? "true" : "false",
                        module_magic_data->float8byval ? "true" : "false");
    }

    // Default message if no specific mismatch found
    if (details.len == 0)
        appendStringInfoString(&details,
                              "Magic block has unexpected length or padding difference.");

    // Report the final error with all details
    ereport(ERROR,
            (errmsg("incompatible library \"%s\": magic block mismatch", libname),
             errdetail_internal("%s", details.data)));
}
```

Key simplifications made:
- Removed redundant newline handling logic in configuration checks
- Consolidated similar parameter checking patterns
- Simplified conditional formatting in version handling
- Removed excessive comments while preserving essential logic flow
- Made the three-tier error checking structure more apparent
- Maintained all critical functionality while improving readability