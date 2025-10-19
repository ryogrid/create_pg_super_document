# json_manifest_array_end

## Location
[src/common/parse_manifest.c:378-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L378-L400)

## Overview
Handles the end of JSON arrays during PostgreSQL backup manifest parsing, transitioning the state back to expect top-level fields.

## Definition
```c
static JsonParseErrorType json_manifest_array_end(void *state)
```

## Detailed Description
This callback function is invoked when the JSON parser encounters the end of an array in a backup manifest. It handles the completion of both "Files" and "WAL-Ranges" arrays by transitioning the parsing state back to expecting top-level fields in the manifest object. The function validates that array endings occur only in appropriate contexts where arrays are expected to complete.

## Parameters / Member Variables
- `state`: A void pointer to the JsonManifestParseState structure containing the current parsing context and state information

## Dependencies
- Functions called/Symbols referenced:
  - [json_manifest_parse_failure](json_manifest_parse_failure.md)
  - JsonManifestParseState (struct)
  - JM_EXPECT_FILES_NEXT (enum value)
  - JM_EXPECT_WAL_RANGES_NEXT (enum value)
  - JM_EXPECT_TOPLEVEL_FIELD (enum value)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - [json_parse_manifest_incremental_init](json_parse_manifest_incremental_init.md)
  - [json_parse_manifest](json_parse_manifest.md)

## Notes and Other Information
- This is a static callback function used specifically within the manifest parsing infrastructure
- The function handles both Files and WAL-Ranges array endings with the same state transition
- Both expected states (JM_EXPECT_FILES_NEXT and JM_EXPECT_WAL_RANGES_NEXT) transition to JM_EXPECT_TOPLEVEL_FIELD
- Error handling is provided for unexpected array endings through json_manifest_parse_failure
- The function always returns JSON_SUCCESS, with errors handled through the parse failure mechanism

## Simplified Source

```c
static JsonParseErrorType json_manifest_array_end(void *state) {
    JsonManifestParseState *parse = state;

    switch (parse->state) {
        case JM_EXPECT_FILES_NEXT:
        case JM_EXPECT_WAL_RANGES_NEXT:
            // End of Files or WAL-Ranges array - return to top level
            parse->state = JM_EXPECT_TOPLEVEL_FIELD;
            break;

        default:
            // Unexpected array end
            json_manifest_parse_failure(parse->context, "unexpected array end");
            break;
    }

    return JSON_SUCCESS;
}
```