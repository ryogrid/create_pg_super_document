# json_out

## Location
[src/backend/utils/adt/json.c:124-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L124-L135)

## Overview
Converts PostgreSQL's internal JSON text representation back to a C-style string for output purposes.

## Definition

```c
Datum
json_out(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the output conversion function for PostgreSQL's JSON data type. It takes PostgreSQL's internal JSON representation (which is stored as text) and converts it to a C-style string that can be returned to the client or used in other contexts where string output is required. This function is the counterpart to  and is typically called when JSON data needs to be displayed or exported from the database.

The function is designed to be efficient and handles detoasting automatically through the  function, which means it can work with both toasted and untoasted text values without additional overhead.

## Parameters / Member Variables
- : PostgreSQL function call context containing:
  - Argument 0: Datum containing the JSON text to be converted to C-string

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts datum argument from function call
  - : Converts text datum to C-string, handling detoasting
  - : Returns C-string to PostgreSQL
- Called from (representative examples):
  - PostgreSQL type input/output system when converting JSON to string representation
  - Client interface functions that need to return JSON as text

## Notes and Other Information
- No explicit detoasting is needed as  handles this automatically
- The function performs a straightforward conversion since JSON is internally stored as text
- Part of PostgreSQL's type input/output infrastructure and automatically invoked during type conversions
- Efficiently handles both compressed (toasted) and uncompressed JSON values
- The output is suitable for display to clients or for further string processing

## Simplified Source

```c
Datum json_out(PG_FUNCTION_ARGS) {
    // Get JSON as datum (detoasting handled by TextDatumGetCString)
    Datum txt = PG_GETARG_DATUM(0);

    // Convert JSON text to C-string for output
    return PG_RETURN_CSTRING(TextDatumGetCString(txt));
}
```