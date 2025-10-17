# text_lt

## Location
[src/backend/utils/adt/varlena.c:1731-1745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1731-L1745)

## Overview
PostgreSQL function implementing the less-than comparison operator (`<`) for text data types by delegating to text_cmp and testing for negative result.

## Definition

```c
Datum
text_lt(PG_FUNCTION_ARGS)
```
## Detailed Description
`text_lt` implements the PostgreSQL `<` operator for text data types. It provides a straightforward wrapper around `text_cmp`, extracting the text arguments and calling the comparison function with the current collation context. The function returns true if the first text argument is lexicographically less than the second according to the specified collation rules. It handles memory management for potentially toasted text values and follows the standard PostgreSQL function call convention.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro for argument access:
  - arg1: First text value (accessed via PG_GETARG_TEXT_PP(0))
  - arg2: Second text value (accessed via PG_GETARG_TEXT_PP(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro)
  - [text_cmp](text_cmp.md)
  - PG_GET_COLLATION
  - PG_FREE_IF_COPY (macro)
  - PG_RETURN_BOOL (macro)
- Called from (representative examples):
  - Currently no direct references found

## Notes and Other Information
- Simple wrapper function providing clean interface for less-than text comparison
- Inherits all collation-aware behavior and optimizations from text_cmp
- Part of the complete set of text comparison operators (=, <>, <, <=, >, >=)
- Essential for text sorting, indexing, and range operations in PostgreSQL
- Proper memory management prevents leaks during repeated comparison operations

## Simplified Source
```c
Datum text_lt(PG_FUNCTION_ARGS)
{
    // Extract text arguments
    text *text1 = PG_GETARG_TEXT_PP(0);
    text *text2 = PG_GETARG_TEXT_PP(1);

    // Perform comparison: result < 0 means text1 < text2
    bool result = (text_cmp(text1, text2, PG_GET_COLLATION()) < 0);

    // Clean up memory if needed
    PG_FREE_IF_COPY(text1, 0);
    PG_FREE_IF_COPY(text2, 1);

    return PG_RETURN_BOOL(result);
}
```