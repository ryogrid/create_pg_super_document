# escape_json

## Location
[src/backend/utils/adt/json.c:1563-1605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1563-L1605)

## Overview
Utility function that produces properly escaped JSON string literals by converting input strings to valid JSON format.

## Definition

```c
void
escape_json(StringInfo buf, const char *str)
```
## Detailed Description
The `escape_json` function transforms a C string into a properly escaped JSON string literal by wrapping the input in double quotes and escaping all special characters according to JSON specification. It handles standard JSON escape sequences (backspace, form feed, newline, carriage return, tab, double quote, backslash) and converts control characters (ASCII < 32) to Unicode escape sequences (\uXXXX format). The function appends the escaped result to a StringInfo buffer.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the escaped JSON string will be appended
- `str`: Input C string to be escaped and converted to JSON string literal format

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfo](../a/appendStringInfo.md)
- Called from (representative examples):
  - [datum_to_json_internal](../d/datum_to_json_internal.md)
  - [composite_to_json](../c/composite_to_json.md)
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

## Simplified Source

```c
// Simplified version of escape_json
void escape_json(StringInfo buf, const char *str) {
    // Add opening quote to create JSON string literal
    appendStringInfoCharMacro(buf, '"');

    // Process each character in input string
    for (const char *p = str; *p; p++) {
        switch (*p) {
            // Handle standard JSON escape sequences
            case '\b': appendStringInfoString(buf, "\\b"); break;
            case '\f': appendStringInfoString(buf, "\\f"); break;
            case '\n': appendStringInfoString(buf, "\\n"); break;
            case '\r': appendStringInfoString(buf, "\\r"); break;
            case '\t': appendStringInfoString(buf, "\\t"); break;
            case '"':  appendStringInfoString(buf, "\\\""); break;
            case '\\': appendStringInfoString(buf, "\\\\"); break;

            default:
                // Handle control characters and regular characters
                if ((unsigned char) *p < ' ') {
                    // Convert control chars to unicode escape sequences
                    appendStringInfo(buf, "\\u%04x", (int) *p);
                } else {
                    // Copy regular characters as-is
                    appendStringInfoCharMacro(buf, *p);
                }
                break;
        }
    }

    // Add closing quote to complete JSON string literal
    appendStringInfoCharMacro(buf, '"');
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Added descriptive comments for each major logic section
- Grouped similar escape sequence cases together
- Clarified the purpose of control character handling
- Made the overall flow more readable while preserving exact functionality
- Maintained all original escape logic and character handling