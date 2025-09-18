# escape_json

## Location
src/backend/utils/adt/json.c: 1563 - 1605

## Overview
Utility function that produces properly escaped JSON string literals by converting input strings to valid JSON format.

## Definition


## Detailed Description
The `escape_json` function transforms a C string into a properly escaped JSON string literal by wrapping the input in double quotes and escaping all special characters according to JSON specification. It handles standard JSON escape sequences (backspace, form feed, newline, carriage return, tab, double quote, backslash) and converts control characters (ASCII < 32) to Unicode escape sequences (\uXXXX format). The function appends the escaped result to a StringInfo buffer.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the escaped JSON string will be appended
- `str`: Input C string to be escaped and converted to JSON string literal format

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro
  - appendStringInfoString
  - appendStringInfo
- Called from (representative examples):
  - datum_to_json_internal
  - composite_to_json
  - json_object
  - json_object_two_arg
  - jsonb_put_escaped_value
  - ExplainProperty
  - write_jsonlog

## Notes and Other Information
- Wraps output in double quotes to create valid JSON string literals
- Implements complete JSON string escaping per RFC specification
- Handles all standard JSON escape sequences: \b, \f, \n, \r, \t, \", \\
- Converts control characters (ASCII < 32) to Unicode escape sequences (\uXXXX)
- Widely used throughout PostgreSQL's JSON/JSONB functionality
- Essential for preventing JSON injection and ensuring syntactic correctness
- Performance-optimized with macro usage for character appending
- Used by both JSON output generation and JSON parsing/transformation functions