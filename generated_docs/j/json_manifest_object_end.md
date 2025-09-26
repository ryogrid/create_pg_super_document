# json_manifest_object_end

## Location
[src/common/parse_manifest.c:317-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L317-L350)

## Overview
Handles the end of JSON objects during PostgreSQL backup manifest parsing, managing state transitions and finalizing file or WAL range entries.

## Definition
```c
static JsonParseErrorType json_manifest_object_end(void *state)
```

## Detailed Description
This callback function is invoked whenever the JSON parser encounters the end of an object in a backup manifest. It manages the parsing state machine by transitioning between different states based on the current context. When ending objects that represent file entries or WAL ranges, it triggers finalization functions to process the collected information. The function handles three main scenarios: completion of the top-level manifest object, completion of individual file objects, and completion of WAL range objects.

## Parameters / Member Variables
- `state`: A void pointer to the JsonManifestParseState structure containing the current parsing context and state information

## Dependencies
- Functions called/Symbols referenced:
  - json_manifest_finalize_file
  - json_manifest_finalize_wal_range  
  - json_manifest_parse_failure
  - JsonManifestParseState (struct)
  - JM_EXPECT_TOPLEVEL_END (enum value)
  - JM_EXPECT_EOF (enum value)
  - JM_EXPECT_THIS_FILE_FIELD (enum value)
  - JM_EXPECT_FILES_NEXT (enum value)
  - JM_EXPECT_THIS_WAL_RANGE_FIELD (enum value)
  - JM_EXPECT_WAL_RANGES_NEXT (enum value)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - json_parse_manifest_incremental_init
  - json_parse_manifest

## Notes and Other Information
- This is a static callback function used specifically within the manifest parsing infrastructure
- The function implements a state machine pattern to track parsing progress through different sections of the manifest
- Error handling is provided for unexpected object endings through json_manifest_parse_failure
- The function always returns JSON_SUCCESS, with errors handled through the parse failure mechanism