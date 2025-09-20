# do_array_start

## Location
[src/test/modules/test_json_parser/test_json_parser_incremental.c:239-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_json_parser/test_json_parser_incremental.c#L239-L249)

## Overview
The  function is a JSON parsing callback function that handles the beginning of JSON arrays in PostgreSQL's incremental JSON parser test module.

## Definition

```c
static JsonParseErrorType
do_array_start(void *state)
```
## Detailed Description
This function is part of the incremental JSON parser test framework in PostgreSQL. It serves as a semantic action callback that gets invoked when the JSON parser encounters the start of an array (opening bracket '['). The function performs two main actions: it outputs the opening bracket to stdout for visual representation of the parsing progress, and it sets a flag in the parser state to indicate that the first array element is being processed. This flag is used to manage comma placement between array elements during the parsing output.

## Parameters / Member Variables
- : A void pointer to the parser state, which gets cast to  internally. This contains the parsing context including lexer information, element tracking flags, and output buffer.

## Dependencies
- Functions called/Symbols referenced:
  -  (struct type for parser state)
  -  (return value constant)
  -  (return type enum)
  -  (standard C library function for output)
- Called from (representative examples):
  - Used as a callback function in JSON parser semantic actions (no direct references found in current analysis)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the test_json_parser_incremental.c file
- Part of the test infrastructure for PostgreSQL's incremental JSON parsing capabilities
- The function always returns , indicating successful processing
- The  flag management is crucial for proper formatting of the output JSON representation
- This function works in conjunction with  and other array/element handling functions to provide complete array parsing semantics