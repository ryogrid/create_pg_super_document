# text_to_array

## Location
src/backend/utils/adt/varlena.c: 4514 - 4539

## Overview
Parses input string and returns a text array of elements based on a provided field separator.

## Definition


## Detailed Description
The text_to_array function is a PostgreSQL built-in function that splits a text string into an array of text elements using a specified delimiter. It initializes a SplitTextOutputData structure and delegates the actual splitting logic to the split_text function. If the splitting operation fails or produces no elements, it handles these cases appropriately by returning NULL or an empty array respectively.

## Parameters / Member Variables
- Takes PostgreSQL function arguments via PG_FUNCTION_ARGS macro (typically text input string and delimiter)
- Returns a Datum representing the resulting text array

## Dependencies
- Functions called/Symbols referenced:
  - SplitTextOutputData (structure for output state)
  - split_text (core text splitting logic)
  - construct_empty_array (creates empty array when no elements)
  - PG_RETURN_ARRAYTYPE_P (macro for returning array type)
  - makeArrayResult (converts array state to result)
  - PG_RETURN_DATUM (macro for returning datum)
- Called from (representative examples):
  - text_to_array_null

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:4514-4539
- Uses memset to initialize the output state structure to all zeroes
- Handles edge cases by returning NULL for failed splits and empty arrays for no elements
- Part of PostgreSQL's variable-length data type utilities