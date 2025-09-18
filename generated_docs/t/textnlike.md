# textnlike

## Location
src/backend/utils/adt/like.c: 303 - 323

## Overview
Implements the NOT LIKE operator for PostgreSQL's text data type, returning true when a text value does not match a given pattern.

## Definition


## Detailed Description
The `textnlike` function provides the implementation for PostgreSQL's NOT LIKE operator when applied to text data types. It takes two text arguments - the string to be tested and the pattern to match against - and uses the generic pattern matching functionality to determine if the text does NOT match the pattern. This is the inverse of the LIKE operation - it returns true when the pattern does not match the text, and false when it does match.

The function extracts the variable-length data and sizes from both text arguments, delegates the actual pattern matching to `GenericMatchText`, and then negates the result to implement the NOT LIKE semantics.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `str` (text): The text value to be tested against the pattern
  - `pat` (text): The pattern to match against, which may contain wildcards like % and _

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Extracts text arguments from function arguments with potential detoasting
  - VARDATA_ANY: Gets pointer to variable-length data for both string and pattern
  - VARSIZE_ANY_EXHDR: Gets size of variable-length data excluding header for both arguments
  - GenericMatchText: Core pattern matching function that handles LIKE operations
  - PG_GET_COLLATION: Gets collation information for the operation
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
  - LIKE_TRUE: Constant representing a successful pattern match
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator framework)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's operator system when the NOT LIKE operator is used with text data types
- The function uses GenericMatchText for the actual pattern matching, ensuring consistent behavior across different LIKE-related functions
- The result is negated compared to regular LIKE operations (returns true when pattern does NOT match)
- Supports collation-aware pattern matching through PG_GET_COLLATION()
- This is the text equivalent of `namenlike`, but operates on variable-length text data instead of fixed-length names
- Handles variable-length text data properly by extracting both data pointers and sizes