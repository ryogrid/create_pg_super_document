# booltext

## Location
[src/backend/utils/adt/bool.c:204-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L204-L222)

## Overview
A cast function that converts PostgreSQL boolean values to text format, following SQL specification behavior.

## Definition
```c
Datum booltext(PG_FUNCTION_ARGS)
```

## Detailed Description
The `booltext` function serves as a cast function to convert boolean values to text representation. Unlike the `boolout()` function, this function follows SQL specification behavior for boolean-to-text conversion, producing lowercase strings "true" and "false" rather than other possible boolean representations like "t"/"f" or "on"/"off".

This function is specifically designed for explicit casting operations where SQL standard compliance is required, ensuring consistent text representation of boolean values across different PostgreSQL operations.

## Parameters / Member Variables
- Input parameter (accessed via `PG_GETARG_BOOL(0)`): The boolean value to be converted to text format

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOOL` - Retrieves boolean argument from function call
  - `[cstring_to_text](../c/cstring_to_text.md)` - Converts C string to PostgreSQL text type
  - `PG_RETURN_TEXT_P` - Returns the text result

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Produces "true" for true values and "false" for false values (lowercase)
- Differs from `boolout()` function which may use different representations
- Specifically designed for SQL-compliant boolean-to-text casting
- Part of PostgreSQL's type casting system
- Located in `src/backend/utils/adt/bool.c` at lines 204-222

## Simplified Source

```c
Datum
booltext(PG_FUNCTION_ARGS)
{
    bool arg1 = PG_GETARG_BOOL(0);
    const char *str;

    // Convert to SQL-compliant text: "true" or "false"
    if (arg1)
        str = "true";
    else
        str = "false";

    PG_RETURN_TEXT_P(cstring_to_text(str));
}
```