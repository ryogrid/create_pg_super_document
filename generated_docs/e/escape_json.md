# escape_json

## Location
[src/backend/utils/adt/json.c:1563-1605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1563-L1605)

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
  - [datum_to_json_internal](../d/datum_to_json_internal.md)
  - composite_to_json
  - [json_object](../j/json_object.md)
  - [json_object_two_arg](../j/json_object_two_arg.md)
  - [jsonb_put_escaped_value](../j/jsonb_put_escaped_value.md)
  - [ExplainProperty](../E/ExplainProperty.md)
  - [write_jsonlog](../w/write_jsonlog.md)

## Notes and Other Information
- Wraps output in double quotes to create valid JSON string literals
- Implements complete JSON string escaping per RFC specification
- Handles all standard JSON escape sequences: \b, \f, \n, \r, \t, \", \\
- Converts control characters (ASCII < 32) to Unicode escape sequences (\uXXXX)
- Widely used throughout PostgreSQL's JSON/JSONB functionality
- Essential for preventing JSON injection and ensuring syntactic correctness
- Performance-optimized with macro usage for character appending
- Used by both JSON output generation and JSON parsing/transformation functions