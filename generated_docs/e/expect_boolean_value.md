# expect_boolean_value

## Location
[src/common/compression.c:311-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/compression.c#L311-L343)

## Overview
A static utility function that parses and validates boolean values for compression options within compression specification parsing.

## Definition

```c
static bool
expect_boolean_value(char *keyword, char *value, pg_compress_specification *result)
```
## Detailed Description
The  function is a helper function used during compression specification parsing to convert string values to boolean values. It accepts various common representations of boolean values including "yes"/"no", "on"/"off", and "1"/"0", performing case-insensitive comparison. If the value is NULL, it defaults to true (allowing boolean flags without explicit values). When an invalid boolean value is provided, it sets an appropriate error message in the result structure's parse_error field. The function is inspired by PostgreSQL's  function.

## Parameters / Member Variables
- : The name of the compression option being parsed (used for error reporting)
- : The string value to be parsed as a boolean (may be NULL, which defaults to true)
- : A pointer to the  structure where parse errors will be recorded

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL case-insensitive string comparison function)
  -  (PostgreSQL string formatting function)
  -  (structure type)
- Called from (representative examples):
  -  (src/common/compression.c:239) - for "long" (long-distance) option

## Notes and Other Information
- Returns the parsed boolean value (true or false)
- NULL value parameter defaults to true, allowing boolean flags without explicit values
- Accepts case-insensitive boolean representations: "yes", "on", "1" for true; "no", "off", "0" for false
- Sets detailed error message in result->parse_error for invalid boolean values
- Error messages are internationalized using the  macro
- Function is declared static, limiting its scope to the compression.c source file
- Inspired by PostgreSQL's  function
- Part of the internal implementation of the compression specification parsing system
- Used specifically for parsing boolean-valued compression options like long-distance mode
- Returns false when parse error occurs, but caller must check result->parse_error to distinguish between legitimate false value and parse failure