# conninfo_array_parse

## Location
src/interfaces/libpq/fe-connect.c: 6029 - 6186

## Overview
Parses parallel arrays of PostgreSQL connection parameter keywords and values, with support for expanding dbname connection strings and applying defaults.

## Definition


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
- : NULL-terminated array of connection parameter keywords (e.g., "host", "port", "dbname")
- : Parallel NULL-terminated array of corresponding parameter values  
- : Buffer for storing detailed error messages if parsing fails
- : Boolean flag indicating whether to add default values for unspecified connection parameters
- : Integer flag controlling dbname expansion behavior (non-zero enables expansion)

## Dependencies
- Functions called/Symbols referenced:
  - recognized_connection_string
  - parse_connection_string
  - conninfo_init
  - PQconninfoFree
  - libpq_append_error
  - conninfo_add_defaults
  - strcmp, strdup, free (standard C library functions)
- Called from (representative examples):
  - internalPQconninfoOption (src/interfaces/libpq/fe-connect.c:415)
  - PQconnectStartParams (src/interfaces/libpq/fe-connect.c:810)

## Notes and Other Information
- This is a static function, internal to the fe-connect.c file
- Supports sophisticated parameter override logic: dbname expansion parameters are applied first, then explicit array parameters override them
- The function only expands the FIRST occurrence of "dbname" as a connection string; subsequent dbname parameters are treated as literal database names
- Proper error handling with detailed messages for invalid connection options
- Memory-safe implementation with comprehensive cleanup on error paths
- Used by higher-level connection functions that accept keyword/value arrays
- The expand_dbname feature is commonly used in command-line applications to allow flexible database specification
- Returns NULL on any error, with details stored in errorMessage buffer