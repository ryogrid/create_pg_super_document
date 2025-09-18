# json_count_array_elements

## Location
src/common/jsonapi.c: 588 - 649

## Overview
A utility function that counts the number of elements in a JSON array by parsing through the array structure without processing the actual element values.

## Definition


## Detailed Description
json_count_array_elements provides a specialized function for determining the number of elements in a JSON array. It operates by creating a shallow copy of the provided lexing context and parsing through the array structure, counting each element encountered. The function is designed to be called from array_start semantic action routines and safely uses a copy of the lexing context to avoid modifying the original parser state. It handles proper array syntax validation, including checking for opening/closing brackets and comma separators between elements.

## Parameters / Member Variables
- : JsonLexContext pointer containing the current parsing state positioned at the start of an array token
- : Pointer to integer that will receive the count of array elements found

## Dependencies
- Functions called/Symbols referenced:
  - lex_expect (for array start/end token validation)
  - lex_peek (for token lookahead)
  - parse_array_element (for processing individual array elements)
  - json_lex (for advancing through tokens)
  - nullSemAction (global semantic action structure for element parsing)
- Called from (representative examples):
  - get_array_start (src/backend/utils/adt/jsonfuncs.c:1311)

## Notes and Other Information
The function uses a shallow copy of the JsonLexContext to safely parse without affecting the original context state. It specifically sets the copied context's strval to NULL since element values are not needed for counting. The lexical level is incremented on the copy to properly track nesting depth during parsing. Empty arrays (containing no elements) are handled correctly and will result in a count of 0.