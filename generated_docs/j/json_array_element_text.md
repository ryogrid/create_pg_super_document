# json_array_element_text

## Location
[src/backend/utils/adt/jsonfuncs.c:963-977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L963-L977)

## Overview
Extracts the specified array element from a JSON array and returns it as text, with JSON string values unescaped and non-string values converted to their text representation.

## Definition

```c
Datum
json_array_element_text(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL built-in function that extracts an element at a specified index from a JSON array. Unlike , this function returns the result as text rather than JSON. For JSON string values, it removes the JSON escaping and quotes. For other JSON values (numbers, booleans, null), it converts them to their text representation.

The function takes a JSON text input and an integer index (zero-based) and uses the common  function with normalization enabled to extract and convert the element to text format.

## Parameters / Member Variables
-  (text): The input JSON array as a PostgreSQL text value
-  (int32): Zero-based index of the array element to extract

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL macro to get text argument
  -  - PostgreSQL macro to get integer argument  
  -  - Common worker function for JSON extraction operations
  -  - PostgreSQL macro to return text result
  -  - PostgreSQL macro to return NULL
- Called from (representative examples):
  - No direct references found (used via SQL function calls)

## Notes and Other Information
- The function uses  with  which means JSON strings are unescaped and converted to plain text
- Returns NULL if the specified index is out of bounds or if the input is not a valid JSON array
- Uses zero-based indexing consistent with JSON array conventions
- Part of PostgreSQL's JSON support introduced to provide text extraction capabilities
- The function is registered as a PostgreSQL built-in function and accessible via SQL

## Simplified Source

```c
Datum json_array_element_text(PG_FUNCTION_ARGS) {
    text *json = PG_GETARG_TEXT_PP(0);
    int element = PG_GETARG_INT32(1);

    // Extract array element as text using common worker function
    text *result = get_worker(json, NULL, &element, 1, true);

    if (result != NULL)
        PG_RETURN_TEXT_P(result);
    else
        PG_RETURN_NULL();
}
```