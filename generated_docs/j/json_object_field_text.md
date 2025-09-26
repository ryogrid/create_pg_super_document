# json_object_field_text

## Location
[src/backend/utils/adt/jsonfuncs.c:882-897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L882-L897)

## Overview
Extracts a field from a JSON object by key name and returns the field value as text, or NULL if the key is not found or the input is not a valid JSON object.

## Definition
```c
Datum json_object_field_text(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the JSON object field access operator (->>). It takes a JSON text value and a text key as input parameters, searches for the specified key within the JSON object, and returns the corresponding value as a text datum. Unlike the -> operator which returns JSON, this operator returns the raw text value without JSON formatting. The function uses the generic get_worker function to perform the actual JSON parsing and field extraction.

The function converts the field name to a C string and delegates the main work to get_worker with parameters indicating single-level extraction and text output format.

## Parameters / Member Variables
- `json`: The input JSON text value from which to extract a field
- `fname`: The text key name to search for in the JSON object

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Retrieves text argument from function call
  - [text_to_cstring](../t/text_to_cstring.md): Converts PostgreSQL text to C string
  - [get_worker](../g/get_worker.md): Generic JSON processing function that handles field extraction
  - PG_RETURN_TEXT_P: Returns text result from function
  - PG_RETURN_NULL: Returns NULL result from function
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- This function is the backend implementation for the JSON ->> operator in SQL
- Returns NULL if the input is not a valid JSON object or if the specified key is not found
- Unlike json_object_field which returns JSON format, this returns plain text
- The function relies on get_worker for the actual JSON parsing and extraction logic
- Located in src/backend/utils/adt/jsonfuncs.c:882-897