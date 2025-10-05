# array_to_json_pretty

## Location
[src/backend/utils/adt/json.c:637-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L637-L653)

## Overview
SQL function that converts a PostgreSQL array into its JSON representation with optional pretty-printing (line feeds and indentation).

## Definition

```c
Datum
array_to_json_pretty(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL SQL function that takes an array and a boolean flag as input, converting the array to a JSON array representation. Unlike , this function accepts a second parameter that controls whether the output should include line feeds for pretty-printing. When the boolean parameter is true, the JSON output will include line breaks and proper indentation for better readability.

The function extracts both the array datum and the boolean flag from the function arguments, creates a StringInfo buffer, delegates the conversion work to  with the appropriate formatting flag, and returns the resulting JSON string as a PostgreSQL text datum.

## Parameters / Member Variables
- Takes two arguments through  macro:

## Dependencies
- Functions called/Symbols referenced:
  -  (macro to extract first function argument)
  -  (macro to extract boolean argument)
  -  (creates a StringInfo buffer)
  -  (performs the actual array-to-JSON conversion)
  -  (converts C string to PostgreSQL text)
  -  (macro to return text result)
- Called from:
  - SQL queries using the  function

## Notes and Other Information
- This function allows control over JSON formatting through the boolean parameter
- When  is true, the output includes line breaks and indentation
- When  is false, the output is compact (same as )
- The function handles all PostgreSQL array types by delegating processing to 
- Located in src/backend/utils/adt/json.c:637-653

## Simplified Source

```c
Datum
array_to_json_pretty(PG_FUNCTION_ARGS)
{
    Datum array = PG_GETARG_DATUM(0);
    bool use_line_feeds = PG_GETARG_BOOL(1);
    StringInfo result;

    // Create output buffer
    result = makeStringInfo();

    // Convert array to JSON format with optional pretty-printing
    array_to_json_internal(array, result, use_line_feeds);

    // Return as PostgreSQL text
    PG_RETURN_TEXT_P(cstring_to_text_with_len(result->data, result->len));
}
``` 