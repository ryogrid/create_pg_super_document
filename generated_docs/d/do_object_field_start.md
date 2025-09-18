# do_object_field_start

## Location
src/test/modules/test_json_parser/test_json_parser_incremental.c: 216 - 230

## Overview
A semantic action callback function used in JSON parsing tests that handles the start of object field names by formatting and outputting the field name with proper JSON escaping and comma separation.

## Definition
```c
static JsonParseErrorType do_object_field_start(void *state, char *fname, bool isnull)
```

## Detailed Description
This function serves as a semantic callback for the JSON parser testing framework, handling the beginning of object field processing. When a JSON object field name is encountered, this function ensures proper formatting by adding comma separators for non-first elements, properly escaping the field name using JSON escape rules, and outputting the field name followed by a colon and space. The function uses a StringInfo buffer for safe string manipulation and maintains element ordering state to ensure correct comma placement in the reconstructed JSON output.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `DoState *` - contains the parsing state including element tracking flags and string buffer
- `fname`: The field name string to be processed and output
- `isnull`: Boolean flag indicating whether the field value is null (parameter appears unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard library)
  - resetStringInfo (PostgreSQL string utility)
  - [escape_json](../e/escape_json.md) (JSON escaping function)
  - [DoState](../D/DoState.md) (struct type)
  - JSON_SUCCESS (return code constant)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - No direct references found (likely referenced through function pointer in parser callbacks)

## Notes and Other Information
- This is a static function within the test_json_parser_incremental.c test module
- Part of a set of semantic callback functions that reconstruct JSON output during parsing
- The function always returns JSON_SUCCESS, indicating successful processing
- Handles comma separation logic by checking `elem_is_first` flag before outputting commas
- Uses PostgreSQL's StringInfo buffer system for safe string operations
- Applies JSON escaping to field names to ensure valid JSON output
- Sets `elem_is_first` to false after processing to affect subsequent element formatting
- The `isnull` parameter is provided but not used in the current implementation