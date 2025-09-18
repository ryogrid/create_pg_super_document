# do_object_start

## Location
src/test/modules/test_json_parser/test_json_parser_incremental.c: 194 - 204

## Overview
A semantic action callback function used in JSON parsing tests that handles the start of JSON objects by outputting the opening brace and resetting state flags.

## Definition
```c
static JsonParseErrorType do_object_start(void *state)
```

## Detailed Description
This function serves as a semantic callback for the JSON parser testing framework. It is invoked when the parser encounters the beginning of a JSON object ('{' character). The function outputs an opening brace to stdout and resets the `elem_is_first` flag in the parser state to true, indicating that the next element will be the first element within this object. This is part of a test harness that reconstructs JSON output while parsing, allowing validation that the parser correctly identifies JSON structure boundaries.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `DoState *` - contains the parsing state including element tracking flags

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard library)
  - [DoState](../D/DoState.md) (struct type)
  - JSON_SUCCESS (return code constant)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - No direct references found (likely referenced through function pointer in parser callbacks)

## Notes and Other Information
- This is a static function within the test_json_parser_incremental.c test module
- Part of a set of semantic callback functions that reconstruct JSON output during parsing
- The function always returns JSON_SUCCESS, indicating successful processing
- The output format includes a newline after the opening brace for readability
- Used in conjunction with other do_* functions to handle different JSON parsing events