# elements_array_element_start

## Location
src/backend/utils/adt/jsonfuncs.c: 2348 - 2369

## Overview
A semantic action callback function invoked when the JSON parser encounters the start of an array element during JSON array processing.

## Definition


## Detailed Description
This function serves as a semantic action callback in PostgreSQL's JSON parsing framework, specifically for handling the beginning of array elements. It is called by the JSON parser when it encounters the start of an array element at the top level (lex_level == 1). The function's primary responsibility is to mark the starting position of the element's value in the input stream for later extraction. It handles two different scenarios: for string tokens in text normalization mode, it sets a flag to indicate the next scalar should be processed specially, while for other cases it records the token start position for subsequent value extraction.

## Parameters / Member Variables
- : Void pointer to ElementsState structure containing parsing context and configuration
- : Boolean indicating whether the array element is null (currently unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - ElementsState: State structure for tracking parsing progress and configuration
  - JSON_TOKEN_STRING: Token type constant for JSON string literals
  - JSON_SUCCESS: Return value indicating successful processing
- Called from:
  - elements_worker: Sets this function as the array_element_start semantic action callback
  - PostgreSQL's JSON parser infrastructure during array processing

## Notes and Other Information
- Only processes elements at lex_level 1 (top-level array elements), ignoring nested structures
- Uses different handling strategies based on normalize_results flag and token type
- For string tokens with normalization enabled, sets next_scalar flag for special processing
- For other tokens, records token_start position for later value extraction
- Returns JSON_SUCCESS to indicate successful processing to the parser
- Part of a coordinated set of semantic actions including elements_array_element_end
- The next_scalar flag is reset by the corresponding array_element_end handler
- Critical for proper JSON array element boundary detection and value extraction