# jsonpath_in

## Location
src/backend/utils/adt/jsonpath.c: 98 - 114

## Overview
The  function is a PostgreSQL input function for the jsonpath data type, responsible for converting a textual string representation of a JSON path expression into the internal jsonpath format.

## Definition


## Detailed Description
 serves as the standard input conversion function for PostgreSQL's jsonpath data type. When a JSON path expression is provided as a string literal in SQL queries, this function is automatically called to parse and convert the textual representation into PostgreSQL's internal jsonpath binary format. The function acts as a simple wrapper that extracts the input string and delegates the actual parsing work to .

The function follows PostgreSQL's standard pattern for type input functions, taking a C-string argument and returning a Datum containing the parsed jsonpath value. It includes basic string length calculation and passes the parsing context for proper memory management.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : The input C-string containing the JSON path expression to be parsed
  - : Memory context for allocation of the resulting jsonpath structure

## Dependencies
- Functions called/Symbols referenced:
  - : Core parsing function that performs the actual string-to-jsonpath conversion
  - : Standard C library function for string length calculation
  - : PostgreSQL macro for extracting C-string arguments
- Called from (representative examples):
  - : Function in JSON table parsing that utilizes jsonpath input conversion

## Notes and Other Information
- This function is automatically invoked by PostgreSQL's type system when converting string literals to jsonpath values
- The actual parsing logic is implemented in , making this function a thin wrapper
- Memory allocation for the resulting jsonpath structure is handled through PostgreSQL's memory context system
- Part of PostgreSQL's JSON path expression support introduced for SQL/JSON standard compliance