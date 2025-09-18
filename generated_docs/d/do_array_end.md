# do_array_end

## Location
src/test/modules/test_json_parser/test_json_parser_incremental.c: 250 - 260

## Overview
The `do_array_end` function is a JSON parsing callback function that handles the end of JSON arrays in PostgreSQL's incremental JSON parser test module.

## Definition
```c
static JsonParseErrorType
do_array_end(void *state)
```

## Detailed Description
This function serves as a semantic action callback that gets invoked when the JSON parser encounters the end of an array (closing bracket ']'). It complements the `do_array_start` function by outputting the closing bracket with proper formatting (including a newline) and resetting the element tracking flag in the parser state. The function ensures that the parser state is properly cleaned up after processing an array, preparing it for subsequent parsing operations.

## Parameters / Member Variables
- `state`: A void pointer to the parser state, which gets cast to `DoState *` internally. This contains the parsing context including lexer information, element tracking flags, and output buffer.

## Dependencies
- Functions called/Symbols referenced:
  - [DoState](../D/DoState.md) (struct type for parser state)
  - `JSON_SUCCESS` (return value constant)
  - `JsonParseErrorType` (return type enum)
  - `printf` (standard C library function for output)
- Called from (representative examples):
  - Used as a callback function in JSON parser semantic actions (no direct references found in current analysis)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the test_json_parser_incremental.c file
- Part of the test infrastructure for PostgreSQL's incremental JSON parsing capabilities
- The function always returns `JSON_SUCCESS`, indicating successful processing
- Sets `elem_is_first` to false, which resets the element state after array completion
- The output includes both a newline before the closing bracket and after it, ensuring proper formatting in the test output
- This function works in conjunction with `do_array_start` and other array/element handling functions to provide complete array parsing semantics