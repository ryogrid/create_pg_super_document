# conninfo_parse

## Location
src/interfaces/libpq/fe-connect.c: 5853 - 6028

## Overview
Parses a PostgreSQL connection string containing key=value pairs and returns a structured array of connection options.

## Definition


## Detailed Description
This function is a core subroutine of  that specifically handles connection strings in key=value pair format. It performs comprehensive parsing of connection parameter strings, handling both quoted and unquoted values, escape sequences, and whitespace normalization.

The parsing process involves several key steps:
1. **Initialization**: Creates a working copy of PQconninfoOptions structure
2. **Tokenization**: Parses the input string to extract parameter name-value pairs
3. **Value Processing**: Handles both quoted (single quotes) and unquoted parameter values
4. **Escape Handling**: Processes backslash escape sequences in values
5. **Storage**: Stores each parsed parameter using 
6. **Default Addition**: Optionally adds default values for unspecified parameters

The function supports two value formats:
- **Unquoted values**: Terminated by whitespace, support backslash escaping
- **Quoted values**: Enclosed in single quotes, support backslash escaping, must be properly terminated

## Parameters / Member Variables
- : The connection string to parse, containing space-separated key=value pairs
- : Buffer for storing detailed error messages if parsing fails
- : Boolean flag indicating whether to add default values for unspecified connection parameters

## Dependencies
- Functions called/Symbols referenced:
  - [conninfo_init](conninfo_init.md)
  - [libpq_append_error](../l/libpq_append_error.md)
  - [PQconninfoFree](../P/PQconninfoFree.md)
  - [conninfo_storeval](conninfo_storeval.md)
  - [conninfo_add_defaults](conninfo_add_defaults.md)
  - strdup, free (standard C library functions)
  - isspace (standard C library function)
- Called from (representative examples):
  - internalPQconninfoOption (src/interfaces/libpq/fe-connect.c:413)
  - [parse_connection_string](../p/parse_connection_string.md) (src/interfaces/libpq/fe-connect.c:5807)

## Notes and Other Information
- This is a static function, meaning it's internal to the fe-connect.c file
- Function performs extensive error checking and provides detailed error messages
- Supports backslash escaping in both quoted and unquoted values
- Memory management: Allocates working buffer that is properly freed on both success and error paths
- Returns NULL on any parsing error, with details stored in errorMessage
- The function modifies a working copy of the input string during parsing
- Quoted strings must be properly terminated or the function will fail with an error
- Whitespace around parameter names, equals signs, and values is properly handled and ignored