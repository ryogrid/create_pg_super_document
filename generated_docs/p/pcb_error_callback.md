# pcb_error_callback

## Location
[src/backend/parser/parse_node.c:170-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_node.c#L170-L188)

## Overview
An error context callback function that inserts parser error location information into error reports occurring during parser operations.

## Definition

```c
static void
pcb_error_callback(void *arg)
```
## Detailed Description
This function serves as an error context callback that automatically adds parser location information to error reports. It is designed to be called for any error occurring while the callback is installed in the error context stack. The function intelligently avoids inserting irrelevant error location information for query cancellation errors, as these typically don't need location context.

The callback extracts the ParseState and location information from the ParseCallbackState structure passed as an argument, then calls parser_errposition() to add location context to the current error, helping developers and users identify where in the SQL query the error occurred.

## Parameters / Member Variables
- : A void pointer that should point to a ParseCallbackState structure containing the parser state and error location information

## Dependencies
- Functions called/Symbols referenced:
  - [ParseCallbackState](../P/ParseCallbackState.md) (structure for callback state)
  - [geterrcode](../g/geterrcode.md)() (gets the current error code)
  - [parser_errposition](parser_errposition.md)() (adds location info to error)
  - ERRCODE_QUERY_CANCELED (error code constant)
- Called from (representative examples):
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md) (installs this callback)

## Notes and Other Information
- This is a static function, only accessible within parse_node.c
- The function specifically checks for ERRCODE_QUERY_CANCELED to avoid adding location info to query cancellation errors
- Used as part of PostgreSQL's error handling infrastructure to provide better error messages with location context
- The callback is typically installed temporarily around calls to non-parser functions that might throw errors
- Location: src/backend/parser/parse_node.c:170-188