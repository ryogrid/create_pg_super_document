# json_manifest_array_start

## Location
[src/common/parse_manifest.c:351-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L351-L377)

## Overview
Handles the start of JSON arrays during PostgreSQL backup manifest parsing, specifically for "Files" and "WAL-Ranges" arrays.

## Definition
```c
static JsonParseErrorType json_manifest_array_start(void *state)
```

## Detailed Description
This callback function is invoked when the JSON parser encounters the beginning of an array in a backup manifest. The function expects only two specific arrays within the top-level manifest object: the "Files" array containing file entries and the "WAL-Ranges" array containing WAL range entries. When a valid array start is encountered, it transitions the parsing state to expect the first element of the respective array. Any unexpected array starts trigger a parse failure.

## Parameters / Member Variables
- `state`: A void pointer to the JsonManifestParseState structure containing the current parsing context and state information

## Dependencies
- Functions called/Symbols referenced:
  - json_manifest_parse_failure
  - JsonManifestParseState (struct)
  - JM_EXPECT_FILES_START (enum value)
  - JM_EXPECT_FILES_NEXT (enum value)
  - JM_EXPECT_WAL_RANGES_START (enum value)
  - JM_EXPECT_WAL_RANGES_NEXT (enum value)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - json_parse_manifest_incremental_init
  - json_parse_manifest

## Notes and Other Information
- This is a static callback function used specifically within the manifest parsing infrastructure
- Only two arrays are expected in the manifest format: "Files" and "WAL-Ranges"
- The function implements strict validation to ensure the manifest structure follows the expected format
- Error handling is provided for any unexpected array starts through json_manifest_parse_failure
- The function always returns JSON_SUCCESS, with errors handled through the parse failure mechanism