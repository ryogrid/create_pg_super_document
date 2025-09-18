# jsonb_put_escaped_value

## Location
[src/backend/utils/adt/jsonb.c:349-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L349-L378)

## Overview
A static utility function that converts a JSONB scalar value to its properly escaped string representation and appends it to a StringInfo buffer.

## Definition


## Detailed Description
This function takes a JSONB scalar value and converts it to its JSON string representation with proper escaping, appending the result to the provided StringInfo buffer. It handles all JSONB scalar types including null, string, numeric, and boolean values. The function ensures that the output conforms to JSON standards by applying appropriate escaping for strings and using standard JSON literals for other types.

## Parameters / Member Variables
- : StringInfo buffer where the escaped JSON representation will be appended
- : Pointer to a JsonbValue structure containing the scalar value to be converted

## Dependencies
- Functions called/Symbols referenced:
  - appendBinaryStringInfo (for null, true, false literals)
  - [escape_json](../e/escape_json.md) (for string escaping)
  - [pnstrdup](../p/pnstrdup.md) (for string duplication)
  - [DatumGetCString](../D/DatumGetCString.md), DirectFunctionCall1, numeric_out (for numeric conversion)
  - elog (for error reporting)
  - jbvNull, jbvString, jbvNumeric, jbvBool (JSONB value type constants)
- Called from (representative examples):
  - [JsonbToCStringWorker](../J/JsonbToCStringWorker.md) (multiple times for different scalar contexts)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (jsonb.c)
- The function uses a switch statement to handle different JSONB scalar types efficiently
- String values are properly escaped using the escape_json function to ensure valid JSON output
- Numeric values are converted using PostgreSQL's numeric_out function for proper formatting
- Boolean values are converted to the standard JSON literals "true" and "false"
- The function will raise an ERROR if an unknown scalar type is encountered
- This function is primarily used as a helper in JSONB to string conversion routines