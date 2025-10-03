# conninfo_array_parse

## Location
[src/interfaces/libpq/fe-connect.c:6029-6186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L6029-L6186)

## Overview
Parses parallel arrays of PostgreSQL connection parameter keywords and values, with support for expanding dbname connection strings and applying defaults.

## Definition

```c
static PQconninfoOption *
conninfo_array_parse(const char *const *keywords, const char *const *values,
					 PQExpBuffer errorMessage, bool use_defaults,
					 int expand_dbname)
```
## Detailed Description
This function is a sophisticated connection parameter parser that processes parallel arrays of connection keywords and their corresponding values. It provides advanced functionality beyond basic parsing, including the ability to expand connection strings found in the "dbname" parameter and merge those parameters with the explicitly provided ones.

Key features include:
1. **Array Processing**: Handles parallel keyword/value arrays until a NULL keyword is encountered
2. **Connection String Expansion**: When  is non-zero, recognizes if the "dbname" value is actually a connection string and parses it
3. **Parameter Precedence**: Later parameters override earlier ones, with explicit parameters taking precedence over expanded dbname parameters
4. **Validation**: Validates that all keywords are recognized connection options
5. **Memory Management**: Properly handles dynamic allocation and cleanup of connection option structures
6. **Default Integration**: Optionally applies default values for unspecified parameters

The dbname expansion feature is particularly useful for command-line tools where users can specify either a simple database name or a full connection string as the database parameter.

## Parameters / Member Variables
- `*keywords`: NULL-terminated array of connection parameter keywords (e.g., "host", "port", "dbname")
- `*values`: Parallel NULL-terminated array of corresponding parameter values
- `errorMessage`: Buffer for storing detailed error messages if parsing fails
- `use_defaults`: Boolean flag indicating whether to add default values for unspecified connection parameters
- `expand_dbname`: Integer flag controlling dbname expansion behavior (non-zero enables expansion)
## Dependencies
- Functions called/Symbols referenced:
  - [recognized_connection_string](../r/recognized_connection_string.md)
  - [parse_connection_string](../p/parse_connection_string.md)
  - [conninfo_init](conninfo_init.md)
  - [PQconninfoFree](../P/PQconninfoFree.md)
  - [libpq_append_error](../l/libpq_append_error.md)
  - [conninfo_add_defaults](conninfo_add_defaults.md)
  - strcmp, strdup, free (standard C library functions)
- Called from (representative examples):
  - internalPQconninfoOption (src/interfaces/libpq/fe-connect.c:415)
  - [PQconnectStartParams](../P/PQconnectStartParams.md) (src/interfaces/libpq/fe-connect.c:810)

## Notes and Other Information
- This is a static function, internal to the fe-connect.c file
- Supports sophisticated parameter override logic: dbname expansion parameters are applied first, then explicit array parameters override them
- The function only expands the FIRST occurrence of "dbname" as a connection string; subsequent dbname parameters are treated as literal database names
- Proper error handling with detailed messages for invalid connection options
- Memory-safe implementation with comprehensive cleanup on error paths
- Used by higher-level connection functions that accept keyword/value arrays
- The expand_dbname feature is commonly used in command-line applications to allow flexible database specification
- Returns NULL on any error, with details stored in errorMessage buffer

## Simplified Source

```c
static PQconninfoOption *
conninfo_array_parse(const char *const *keywords, const char *const *values,
                     PQExpBuffer errorMessage, bool use_defaults,
                     int expand_dbname)
{
    PQconninfoOption *options;
    PQconninfoOption *dbname_options = NULL;
    int i = 0;

    // Phase 1: Check if dbname value is a connection string and parse it
    if (expand_dbname) {
        while (keywords[i]) {
            if (strcmp(keywords[i], "dbname") == 0 && values[i]) {
                if (recognized_connection_string(values[i])) {
                    dbname_options = parse_connection_string(values[i], errorMessage, false);
                    if (dbname_options == NULL)
                        return NULL;
                }
                break;
            }
            i++;
        }
    }

    // Phase 2: Initialize connection options structure
    options = conninfo_init(errorMessage);
    if (options == NULL) {
        PQconninfoFree(dbname_options);
        return NULL;
    }

    // Phase 3: Process keyword/value pairs
    i = 0;
    while (keywords[i]) {
        const char *pname = keywords[i];
        const char *pvalue = values[i];

        if (pvalue != NULL && pvalue[0] != '\0') {
            // Find the option in the options array
            PQconninfoOption *option;
            for (option = options; option->keyword != NULL; option++) {
                if (strcmp(option->keyword, pname) == 0)
                    break;
            }

            // Validate keyword
            if (option->keyword == NULL) {
                libpq_append_error(errorMessage, "invalid connection option \"%s\"", pname);
                goto error_cleanup;
            }

            // Special handling for dbname expansion
            if (strcmp(pname, "dbname") == 0 && dbname_options) {
                // Copy all parsed dbname parameters to main options
                for (PQconninfoOption *str_option = dbname_options;
                     str_option->keyword != NULL; str_option++) {
                    if (str_option->val != NULL) {
                        copy_option_value(options, str_option);
                    }
                }
                PQconninfoFree(dbname_options);
                dbname_options = NULL;
            } else {
                // Store regular parameter value
                free(option->val);
                option->val = strdup(pvalue);
                if (!option->val) {
                    libpq_append_error(errorMessage, "out of memory");
                    goto error_cleanup;
                }
            }
        }
        i++;
    }

    // Phase 4: Apply defaults if requested
    if (use_defaults) {
        if (!conninfo_add_defaults(options, errorMessage)) {
            PQconninfoFree(options);
            return NULL;
        }
    }

    return options;

error_cleanup:
    PQconninfoFree(options);
    PQconninfoFree(dbname_options);
    return NULL;
}
```