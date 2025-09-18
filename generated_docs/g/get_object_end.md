# get_object_end

## Location
src/backend/utils/adt/jsonfuncs.c: 1177 - 1194

## Overview
A JSON semantic action callback function that handles the end of JSON object parsing and extracts the complete object text when the entire root object is being matched.

## Definition
static JsonParseErrorType get_object_end(void *state)

## Detailed Description
The `get_object_end` function is a semantic action callback used during JSON parsing to handle the completion of JSON objects. It performs a special case operation for when the entire outermost object should be extracted (when lex_level is 0 and npath is 0). In this scenario, it calculates the length of the complete object by using the previously recorded start position and the current token terminator, then converts this text range into a PostgreSQL text datum for return. This function works as a pair with `get_object_start` to capture complete JSON objects.

## Parameters / Member Variables
- `state`: A void pointer to the GetState structure containing parsing context and state information

## Dependencies
- Functions called/Symbols referenced:
  - GetState
  - cstring_to_text_with_len
  - JSON_SUCCESS
  - JsonParseErrorType
- Called from (representative examples):
  - get_worker

## Notes and Other Information
- This function is static and internal to jsonfuncs.c
- It only performs meaningful work at the outermost lexical level (lex_level == 0) when no path is specified (npath == 0)
- The function calculates the object length by subtracting the start position from the previous token terminator position
- The extracted text is converted to PostgreSQL's text type using cstring_to_text_with_len
- Works in conjunction with get_object_start which records the initial position
- Always returns JSON_SUCCESS to indicate successful processing