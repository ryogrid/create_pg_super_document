# text_concat_ws

## Location
[src/backend/utils/adt/varlena.c:5517-5537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5517-L5537)

## Overview
PostgreSQL built-in function that concatenates arguments with a specified separator string, where the first argument serves as the separator for the remaining arguments.

## Definition
```c
Datum text_concat_ws(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQL's concat_ws() SQL function (concatenate with separator). The first argument serves as the separator string, and all subsequent arguments are concatenated using this separator. The function converts the first argument from text to a C string, then delegates the actual concatenation work to concat_internal starting from argument index 1. If the separator argument is NULL, the function immediately returns NULL. Other NULL arguments in the value list are ignored during concatenation.

## Parameters / Member Variables
- First argument (index 0): separator string
- Remaining arguments (index 1+): values to concatenate

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](text_to_cstring.md)
  - [concat_internal](../c/concat_internal.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - (No direct references found - called via SQL function dispatch)

## Notes and Other Information
- Implements the SQL concat_ws() function which concatenates with separators
- Returns NULL immediately if the separator (first argument) is NULL
- NULL value arguments are ignored, but not the separator
- The separator is placed between non-NULL values only
- Uses standard PostgreSQL function calling conventions
- Part of the text/varchar data type implementation in PostgreSQL
- The 'ws' suffix stands for 'with separator'