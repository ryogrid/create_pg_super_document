# text_smaller

## Location
[src/backend/utils/adt/varlena.c:2583-2599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2583-L2599)

## Overview
A comparison function that returns the lexicographically smaller of two text values according to the specified collation.

## Definition
Datum text_smaller(PG_FUNCTION_ARGS)

## Detailed Description
This function implements a min operation for text data types. It compares two text arguments using the current collation and returns whichever text value is lexicographically smaller. The comparison is collation-aware, meaning the result depends on the locale-specific sorting rules of the collation in use. This function is typically used in aggregate operations or as a support function for operators that need to determine the minimum text value.

## Parameters / Member Variables
- arg1: First text value to compare (retrieved via PG_GETARG_TEXT_PP(0))
- arg2: Second text value to compare (retrieved via PG_GETARG_TEXT_PP(1))
- result: Pointer to the smaller of the two input text values

## Dependencies
- Functions called/Symbols referenced:
  - [text_cmp](text_cmp.md)
  - PG_GET_COLLATION
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found in the codebase (likely used through SQL function calls)

## Notes and Other Information
- Uses text_cmp for the actual comparison logic with collation support
- Returns a pointer to one of the input arguments rather than creating a new text object for efficiency
- The function respects locale-specific collation rules for text comparison
- Commonly used in MIN aggregate functions or comparison operators for text types
- Complementary function to text_larger, using less-than comparison instead of greater-than
- Located in src/backend/utils/adt/varlena.c:2583-2599