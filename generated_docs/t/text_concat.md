# text_concat

## Location
[src/backend/utils/adt/varlena.c:5502-5516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5502-L5516)

## Overview
PostgreSQL built-in function that concatenates all provided arguments without any separator, ignoring NULL arguments.

## Definition
```c
Datum text_concat(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the PostgreSQL SQL function entry point for the concat() operation. It acts as a thin wrapper around concat_internal, specifically configured to concatenate arguments without any separator (empty string). The function processes all arguments starting from index 0, ignores NULL values, and returns the concatenated result as a text datum. If the internal concatenation results in NULL, the function properly returns a SQL NULL value.

## Parameters / Member Variables
- Uses PostgreSQL's standard PG_FUNCTION_ARGS macro to access function call information and arguments

## Dependencies
- Functions called/Symbols referenced:
  - [concat_internal](../c/concat_internal.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - (No direct references found - called via SQL function dispatch)

## Notes and Other Information
- Implements the SQL concat() function which concatenates multiple values without separators
- NULL arguments are ignored during concatenation
- Returns NULL if the internal concatenation operation determines the result should be NULL
- Uses standard PostgreSQL function calling conventions with PG_FUNCTION_ARGS and PG_RETURN_* macros
- Part of the text/varchar data type implementation in PostgreSQL