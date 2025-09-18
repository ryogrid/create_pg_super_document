# bttextcmp

## Location
src/backend/utils/adt/varlena.c: 1831 - 1845

## Overview
A PostgreSQL function that provides B-tree comparison functionality for text data types, returning an integer indicating the lexicographic ordering relationship between two text values.

## Definition


## Detailed Description
The `bttextcmp` function is a PostgreSQL built-in function that implements B-tree comparison semantics for text data types. It serves as a comparison function specifically designed for use in B-tree index operations, returning a tri-state integer result: negative if the first argument is less than the second, zero if they are equal, and positive if the first argument is greater than the second. The function leverages the core `text_cmp` function to perform collation-aware comparison while providing the standardized B-tree comparison interface that PostgreSQL's indexing system expects.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL's standard macro for function arguments, containing:
  - `arg1` (text*): First text value to compare (left operand)
  - `arg2` (text*): Second text value to compare (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - `[text_cmp](../t/text_cmp.md)`: Core text comparison function that performs collation-aware string comparison
  - `PG_GET_COLLATION`: Retrieves the collation to use for the comparison
  - `PG_GETARG_TEXT_PP`: Macro to extract text arguments from function call
  - `PG_FREE_IF_COPY`: Memory management macro to free copied arguments if necessary
  - `PG_RETURN_INT32`: Macro to return 32-bit integer result as Datum
- Called from (representative examples):
  - No direct references found (typically used in B-tree index support functions and operator classes)

## Notes and Other Information
- This function provides the foundation for B-tree indexing of text columns in PostgreSQL
- Returns standard tri-state comparison result: <0, 0, or >0 for less-than, equal, or greater-than respectively
- Uses collation-aware comparison, respecting locale-specific sorting rules through `text_cmp`
- Essential component of PostgreSQL's text indexing infrastructure
- Properly handles memory management by freeing copied arguments after use
- Part of the operator class support functions that enable efficient text indexing and sorting
- The function is defined in `src/backend/utils/adt/varlena.c` at lines 1831-1845