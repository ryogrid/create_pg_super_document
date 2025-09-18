# text_gt

## Location
src/backend/utils/adt/varlena.c: 1761 - 1775

## Overview
A PostgreSQL function that implements the "greater than" comparison operator (>) for the text data type, returning true if the first text argument is lexicographically greater than the second.

## Definition


## Detailed Description
The `text_gt` function is a PostgreSQL built-in function that performs a "greater than" comparison between two text values. It uses collation-aware comparison through the `text_cmp` function to determine the lexicographic ordering. The function follows PostgreSQL's standard function calling convention for built-in functions, accepting arguments through the `PG_FUNCTION_ARGS` macro and returning a `Datum` type. The comparison result is true (1) if the first text argument is lexicographically greater than the second argument, and false (0) otherwise.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL's standard macro for function arguments, containing:
  - `arg1` (text*): First text value to compare (left operand)
  - `arg2` (text*): Second text value to compare (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - [text_cmp](text_cmp.md): Core text comparison function that performs collation-aware string comparison
  - `PG_GET_COLLATION`: Retrieves the collation to use for the comparison
  - `PG_GETARG_TEXT_PP`: Macro to extract text arguments from function call
  - `PG_FREE_IF_COPY`: Memory management macro to free copied arguments if necessary
  - `PG_RETURN_BOOL`: Macro to return boolean result as Datum
- Called from (representative examples):
  - No direct references found (typically called through SQL operator > for text types)

## Notes and Other Information
- This function implements the PostgreSQL > operator for text data types
- Uses collation-aware comparison, respecting locale-specific sorting rules
- Properly handles memory management by freeing copied arguments after use
- Part of PostgreSQL's comprehensive set of text comparison operators
- The function is defined in `src/backend/utils/adt/varlena.c` at lines 1761-1775