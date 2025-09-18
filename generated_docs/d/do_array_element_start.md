# do_array_element_start

## Location
[src/test/modules/test_json_parser/test_json_parser_incremental.c:261-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_json_parser/test_json_parser_incremental.c#L261-L272)

## Overview
The `do_array_element_start` function is a JSON parsing callback function that handles the beginning of individual elements within JSON arrays in PostgreSQL's incremental JSON parser test module.

## Definition
```c
static JsonParseErrorType
do_array_element_start(void *state, bool isnull)
```

## Detailed Description
This function serves as a semantic action callback that gets invoked when the JSON parser begins processing an individual element within an array. Its primary responsibility is to manage the comma-separated formatting of array elements in the output. The function checks whether this is the first element in the array using the `elem_is_first` flag, and if it's not the first element, it outputs a comma and newline to properly separate array elements. After processing, it always sets the `elem_is_first` flag to false, ensuring that subsequent elements will be properly comma-separated.

## Parameters / Member Variables
- `state`: A void pointer to the parser state, which gets cast to `DoState *` internally. This contains the parsing context including lexer information, element tracking flags, and output buffer.
- `isnull`: A boolean parameter indicating whether the array element is null. This parameter is received but not used in the current implementation.

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
- The `isnull` parameter is not currently utilized in the implementation but is part of the callback interface
- Critical for maintaining proper JSON array formatting in the test output by managing comma placement
- Works in coordination with `do_array_start`, `do_array_end`, and `do_array_element_end` functions
- The `elem_is_first` flag management ensures that the first array element doesn't get preceded by a comma