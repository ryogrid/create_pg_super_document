# textoverlay

## Location
src/backend/utils/adt/varlena.c: 1093 - 1103

## Overview
A PostgreSQL function that implements the SQL OVERLAY() operation, replacing a specified substring within the first text argument with the second text argument.

## Definition


## Detailed Description
This function provides the PostgreSQL implementation of the SQL standard OVERLAY() function. It extracts arguments from the PostgreSQL function call interface and delegates the actual overlay operation to the internal  function. The function follows the SQL standard definition, which conceptually works by:

1. Taking a substring from the beginning of the first text up to the start position
2. Appending the replacement text 
3. Appending the remainder of the first text after the specified length

The function serves as a wrapper that handles PostgreSQL's function calling conventions and argument extraction, then passes the work to the more specialized internal implementation.

## Parameters / Member Variables
-  (t1): The original text string to be modified
-  (t2): The replacement text to insert
-  (sp): The substring start position (1-based)
-  (sl): The substring length to replace

## Dependencies
- Functions called/Symbols referenced:
  - [text_overlay](text_overlay.md)
  - PG_RETURN_TEXT_P
  - PG_GETARG_TEXT_PP
  - PG_GETARG_INT32
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL OVERLAY function)

## Notes and Other Information
- Implements the SQL standard OVERLAY() function as defined in SQL specifications
- Uses PostgreSQL's standard function calling conventions with PG_FUNCTION_ARGS
- The actual overlay logic is delegated to the internal  function
- Part of PostgreSQL's variable-length character data handling utilities
- Located in src/backend/utils/adt/varlena.c with other text manipulation functions
- The function signature matches PostgreSQL's C function interface for SQL-callable functions