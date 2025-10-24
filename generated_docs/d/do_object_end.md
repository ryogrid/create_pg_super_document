# do_object_end

## Location
[src/test/modules/test_json_parser/test_json_parser_incremental.c:205-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_json_parser/test_json_parser_incremental.c#L205-L215)

## Overview
A semantic action callback function used in JSON parsing tests that handles the end of JSON objects by outputting the closing brace and updating state flags.

## Definition
```c
static JsonParseErrorType do_object_end(void *state)
```

## Detailed Description
This function serves as a semantic callback for the JSON parser testing framework, complementing `do_object_start`. It is invoked when the parser encounters the end of a JSON object ('}' character). The function outputs a closing brace with proper formatting (newline before and after the brace) and sets the `elem_is_first` flag in the parser state to false, indicating that subsequent elements are not the first in their container. This maintains proper state tracking as the parser exits object boundaries during JSON reconstruction.

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
- Output includes newlines for proper JSON formatting and readability
- Works in conjunction with `do_object_start` to handle complete object boundaries
- Sets `elem_is_first` to false, which affects how subsequent elements are formatted

## Simplified Source

```c
static JsonParseErrorType
do_object_end(void *state)
{
    DoState *_state = (DoState *) state;

    // Output closing brace for JSON object
    printf("\n}\n");

    // Clear first element flag
    _state->elem_is_first = false;

    return JSON_SUCCESS;
}
```