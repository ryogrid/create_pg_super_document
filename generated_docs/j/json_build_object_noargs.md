# json_build_object_noargs

## Location
[src/backend/utils/adt/json.c:1329-1334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1329-L1334)

## Overview
The json_build_object_noargs function is a degenerate case of json_build_object that handles the scenario when no arguments are provided, returning an empty JSON object.

## Definition
```c
Datum json_build_object_noargs(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a specialized version of json_build_object for the case where zero arguments are passed. Instead of going through the complex variadic argument processing, it directly returns an empty JSON object represented as the string "{}". This optimization provides a more efficient path for creating empty JSON objects.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function calling convention that provides access to the function call information, though no arguments are expected in this case

## Dependencies
- Functions called/Symbols referenced:
  - cstring_to_text_with_len: Converts a C string to PostgreSQL text type with specified length
  - PG_RETURN_TEXT_P: PostgreSQL macro for returning a text pointer value

- Called from (representative examples):
  - No direct references found in the codebase (called via SQL interface)

## Notes and Other Information
- This function is a performance optimization for the zero-argument case of json_build_object
- It directly returns the string "{}" as a text object with length 2
- The function avoids the overhead of variadic argument processing when no arguments are provided
- This is part of PostgreSQL JSON data type support system located in src/backend/utils/adt/json.c:1329-1334
- The comment in the source explicitly mentions this as a "degenerate case" of json_build_object