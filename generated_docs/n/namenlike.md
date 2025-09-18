# namenlike

## Location
src/backend/utils/adt/like.c: 261 - 281

## Overview
Implements the NOT LIKE operator for PostgreSQL's Name data type, returning true when a name does not match a given text pattern.

## Definition


## Detailed Description
The  function provides the implementation for PostgreSQL's NOT LIKE operator when applied to Name data types. It takes a Name value and a text pattern as arguments, then uses the generic pattern matching functionality to determine if the name does NOT match the pattern. This is the inverse of the LIKE operation - it returns true when the pattern does not match the name, and false when it does match.

The function extracts the C string from the Name argument, gets the pattern from the text argument, and delegates the actual pattern matching to . The result is then negated to implement the NOT LIKE semantics.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (Name): The name value to be tested against the pattern
  -  (text): The pattern to match against, which may contain wildcards

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extracts Name argument from function arguments
  - PG_GETARG_TEXT_PP: Extracts text pattern argument with potential detoasting
  - NameStr: Macro to get C string from Name structure
  - VARDATA_ANY: Gets pointer to variable-length data
  - VARSIZE_ANY_EXHDR: Gets size of variable-length data excluding header
  - GenericMatchText: Core pattern matching function that handles LIKE operations
  - PG_GET_COLLATION: Gets collation information for the operation
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
  - LIKE_TRUE: Constant representing a successful pattern match
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator framework)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's operator system when the NOT LIKE operator is used with Name data types
- The function uses GenericMatchText for the actual pattern matching, ensuring consistent behavior across different LIKE-related functions
- The result is negated compared to regular LIKE operations (returns true when pattern does NOT match)
- Supports collation-aware pattern matching through PG_GET_COLLATION()