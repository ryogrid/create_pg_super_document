# boolin

## Location
[src/backend/utils/adt/bool.c:126-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L126-L156)

## Overview
PostgreSQL input function for the boolean data type that converts string representations to internal boolean values with proper error handling and whitespace trimming.

## Definition


## Detailed Description
The `boolin` function serves as the input conversion function for PostgreSQL's boolean data type. It is automatically called by the PostgreSQL type system when converting string inputs to boolean values during SQL operations. The function handles whitespace normalization by trimming leading and trailing spaces, then delegates the actual parsing to `parse_bool_with_len`. If parsing fails, it raises a properly formatted error using PostgreSQL's error reporting system with the ERRCODE_INVALID_TEXT_REPRESENTATION error code.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access function call context
- Input parameter accessed via `PG_GETARG_CSTRING(0)`: The string representation to convert to boolean

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (PostgreSQL function argument extraction macro)
  - isspace (standard C library function)
  - strlen (standard C library function) 
  - [parse_bool_with_len](../p/parse_bool_with_len.md) (core boolean parsing function)
  - PG_RETURN_BOOL (PostgreSQL return value macro)
  - ereturn (PostgreSQL error reporting function)
  - [errcode](../e/errcode.md) (PostgreSQL error code function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message function)
- Called from:
  - PostgreSQL type system (no direct references in indexed code)

## Notes and Other Information
- This is a PostgreSQL "input function" registered in the system catalogs for the boolean data type
- Automatically invoked when PostgreSQL needs to convert text to boolean (e.g., INSERT, UPDATE, CAST operations)
- Performs comprehensive whitespace handling by trimming both leading and trailing spaces
- Returns a Datum (PostgreSQL's generic data type) containing the boolean value
- Raises detailed error messages on parse failure, including the original input string for debugging
- Uses PostgreSQL's standard error reporting mechanism for consistent error handling
- The function signature follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS