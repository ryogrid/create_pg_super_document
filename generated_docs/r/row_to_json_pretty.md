# row_to_json_pretty

## Location
[src/backend/utils/adt/json.c:670-690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L670-L690)

## Overview
SQL function that converts a PostgreSQL composite type (record/row) into its JSON object representation with optional pretty-printing (line feeds and indentation).

## Definition

```c
Datum
row_to_json_pretty(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL SQL function that takes a composite type (record/row) and a boolean flag as input, converting the composite type to a JSON object representation. Unlike , this function accepts a second parameter that controls whether the output should include line feeds for pretty-printing. When the boolean parameter is true, the JSON output will include line breaks and proper indentation for better readability.

The function extracts both the composite datum and the boolean flag from the function arguments, creates a StringInfo buffer, delegates the conversion work to  with the appropriate formatting flag, and returns the resulting JSON string as a PostgreSQL text datum. The resulting JSON object will have field names as keys and field values as JSON values.

## Parameters / Member Variables
- Takes two arguments through  macro:

## Dependencies
- Functions called/Symbols referenced:
  -  (macro to extract first function argument)
  -  (macro to extract boolean argument)
  -  (creates a StringInfo buffer)
  -  (performs the actual composite-to-JSON conversion)
  -  (converts C string to PostgreSQL text)
  -  (macro to return text result)
- Called from:
  - SQL queries using the  function

## Notes and Other Information
- This function allows control over JSON formatting through the boolean parameter
- When  is true, the output includes line breaks and proper field separation
- When  is false, the output is compact (same as )
- Field names become JSON object keys, and field values are converted to appropriate JSON values
- Dropped columns are automatically excluded from the JSON output
- NULL values are represented as JSON 
- The function handles all PostgreSQL composite types by delegating processing to 
- Located in 