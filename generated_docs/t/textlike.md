# textlike

## Location
[src/backend/utils/adt/like.c:282-302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L282-L302)

## Overview
Implements the LIKE operator for PostgreSQL's text data type, returning true when a text value matches a given pattern.

## Definition


## Detailed Description
The `textlike` function provides the implementation for PostgreSQL's LIKE operator when applied to text data types. It takes two text arguments - the string to be tested and the pattern to match against - and uses the generic pattern matching functionality to determine if the text matches the pattern. This function returns true when the pattern matches the text, and false when it does not match.

The function extracts the variable-length data and sizes from both text arguments, then delegates the actual pattern matching to `GenericMatchText`. The result directly reflects the pattern matching outcome without negation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `str` (text): The text value to be tested against the pattern
  - `pat` (text): The pattern to match against, which may contain wildcards like % and _

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Extracts text arguments from function arguments with potential detoasting
  - VARDATA_ANY: Gets pointer to variable-length data for both string and pattern
  - VARSIZE_ANY_EXHDR: Gets size of variable-length data excluding header for both arguments
  - [GenericMatchText](../G/GenericMatchText.md): Core pattern matching function that handles LIKE operations
  - PG_GET_COLLATION: Gets collation information for the operation
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
  - LIKE_TRUE: Constant representing a successful pattern match
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator framework)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's operator system when the LIKE operator is used with text data types
- The function uses GenericMatchText for the actual pattern matching, ensuring consistent behavior across different LIKE-related functions
- Supports collation-aware pattern matching through PG_GET_COLLATION()
- Unlike `namenlike`, this function returns the direct result of the pattern matching without negation
- Handles variable-length text data properly by extracting both data pointers and sizes