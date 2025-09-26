# jsonb_pretty

## Location
[src/backend/utils/adt/jsonfuncs.c:4583-4598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4583-L4598)

## Overview
Converts a JSONB value into a pretty-printed, human-readable text representation with proper indentation and formatting.

## Definition
```c
Datum jsonb_pretty(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_pretty` function is a SQL function that takes a JSONB value and returns a formatted text representation that is more readable than the compact JSON representation. It uses indentation, line breaks, and spacing to make the JSON structure visually clear and easy to read.

The function leverages `JsonbToCStringIndent` to perform the actual formatting work, which handles proper indentation levels for nested objects and arrays.

## Parameters / Member Variables
- `jb`: Input JSONB value to be pretty-printed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - [makeStringInfo](../m/makeStringInfo.md)
  - [JsonbToCStringIndent](../J/JsonbToCStringIndent.md)
  - VARSIZE
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct callers found (exposed as SQL function)

## Notes and Other Information
- Returns a PostgreSQL TEXT type containing the formatted JSON
- Uses `JsonbToCStringIndent` for the core formatting logic
- The output includes proper indentation, line breaks, and spacing for readability
- Useful for debugging and displaying JSON data in a human-friendly format
- Exposed as the SQL function `jsonb_pretty(jsonb)`