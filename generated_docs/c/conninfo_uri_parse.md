# conninfo_uri_parse

## Location
src/interfaces/libpq/fe-connect.c: 6322 - 6374

## Overview
Parses a PostgreSQL URI connection string and returns a structured array of connection options with optional defaults.

## Definition


## Detailed Description
This function is a subroutine of  specifically designed to handle PostgreSQL connection strings in URI format (e.g., "postgresql://user:password@host:port/database?param=value"). It provides a clean interface for URI parsing while delegating the actual parsing work to .

The function follows a standard pattern used throughout the connection string parsing subsystem:
1. **Initialization**: Creates a working copy of the PQconninfoOptions template
2. **URI Parsing**: Delegates actual URI parsing to 
3. **Default Population**: Optionally adds default values for unspecified parameters
4. **Error Handling**: Ensures proper cleanup on any failure

This design separates the high-level parsing orchestration from the detailed URI syntax parsing logic, making the code more maintainable and testable.

## Parameters / Member Variables
- : The URI-format connection string to parse (e.g., "postgresql://localhost:5432/mydb")
- : Buffer for storing detailed error messages if parsing fails
- : Boolean flag indicating whether to populate unspecified parameters with default values

## Dependencies
- Functions called/Symbols referenced:
  - conninfo_init
  - conninfo_uri_parse_options
  - PQconninfoFree
  - conninfo_add_defaults
- Called from (representative examples):
  - internalPQconninfoOption (src/interfaces/libpq/fe-connect.c:420)
  - parse_connection_string (src/interfaces/libpq/fe-connect.c:5804)

## Notes and Other Information
- This is a static function, internal to the fe-connect.c file
- Acts as a wrapper around , providing consistent interface with other parsing functions
- Follows the same error handling and memory management patterns as  and 
- Returns NULL on any parsing error, with details stored in errorMessage
- Part of PostgreSQL's multi-format connection string parsing system that supports both key=value and URI formats
- The actual URI parsing logic is implemented in 
- Supports the standard PostgreSQL URI schemes: "postgresql://" and "postgres://"
- Memory management is handled consistently with other parsing functions: allocates result on success, cleans up on failure