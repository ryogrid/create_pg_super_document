# quote_nullable

## Location
src/backend/utils/adt/quote.c: 125 - 132

## Overview
A PostgreSQL built-in function that handles both NULL values and regular text by returning the string 'NULL' for null inputs or a properly quoted literal for non-null text inputs.

## Definition


## Detailed Description
The `quote_nullable` function extends the functionality of `quote_literal` by handling NULL values appropriately. When the input argument is NULL, it returns the text string 'NULL' (without quotes), which is the correct SQL representation for NULL values. For non-NULL inputs, it delegates to the `quote_literal` function to perform standard literal quoting and escaping. This function is particularly useful when constructing dynamic SQL queries where NULL values need to be represented correctly as the unquoted NULL keyword rather than as quoted string literals.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: `text` (nullable) - The input text to be quoted, or NULL

## Dependencies
- Functions called/Symbols referenced:
  - `cstring_to_text` - Converts C string to PostgreSQL text type
  - `PG_RETURN_TEXT_P` - Macro for returning text values from PostgreSQL functions
  - `quote_literal` - Function for quoting non-NULL text literals
  - `PG_RETURN_DATUM` - Macro for returning generic PostgreSQL datums
  - `DirectFunctionCall1` - Macro for calling PostgreSQL functions directly
- Called from (representative examples):
  - No direct references found in the codebase (likely called from SQL queries)

## Notes and Other Information
- This function is exposed as a SQL function that can be called directly from SQL queries
- Handles the distinction between NULL values (returns 'NULL') and empty strings (returns quoted empty string)
- Essential for dynamic SQL generation where NULL handling is required
- Part of PostgreSQL's quote utility functions located in `src/backend/utils/adt/quote.c`
- The NULL case returns the literal string 'NULL' without any quotes, which is correct SQL syntax
- For non-NULL inputs, uses `DirectFunctionCall1` to efficiently call the `quote_literal` function
- Critical for applications that need to distinguish between NULL values and empty strings in dynamic queries