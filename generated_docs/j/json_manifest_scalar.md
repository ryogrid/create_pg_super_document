# json_manifest_scalar

## Location
src/common/parse_manifest.c: 517 - 595

## Overview
Handles scalar values during PostgreSQL backup manifest parsing, processing and storing field values based on the current parsing context.

## Definition
```c
static JsonParseErrorType json_manifest_scalar(void *state, char *token, JsonTokenType tokentype)
```

## Detailed Description
This callback function is invoked when the JSON parser encounters scalar values (strings, numbers, booleans) in a backup manifest. The function processes values based on the current parsing state, which was established by previous calls to json_manifest_object_field_start. It handles values for top-level manifest fields (version, system identifier, manifest checksum), file object fields (path, encoded path, size, last modified, checksum algorithm, checksum), and WAL range object fields (timeline, start LSN, end LSN). For version and system identifier fields, the function immediately calls finalization functions for validation, while other values are stored in the parse state for later processing when the containing object ends.

## Parameters / Member Variables
- `state`: A void pointer to the JsonManifestParseState structure containing the current parsing context and state information
- `token`: The scalar value as a string that was parsed from the JSON document
- `tokentype`: The JsonTokenType indicating the type of the scalar token (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - json_manifest_finalize_version
  - json_manifest_finalize_system_identifier
  - json_manifest_parse_failure
  - pfree
  - JsonManifestParseState (struct)
  - JsonTokenType (enum)
  - JM_EXPECT_VERSION_VALUE (enum value)
  - JM_EXPECT_SYSTEM_IDENTIFIER_VALUE (enum value)
  - JM_EXPECT_TOPLEVEL_FIELD (enum value)
  - JM_EXPECT_THIS_FILE_VALUE (enum value)
  - JM_EXPECT_THIS_FILE_FIELD (enum value)
  - JM_EXPECT_THIS_WAL_RANGE_VALUE (enum value)
  - JM_EXPECT_THIS_WAL_RANGE_FIELD (enum value)
  - JM_EXPECT_MANIFEST_CHECKSUM_VALUE (enum value)
  - JM_EXPECT_TOPLEVEL_END (enum value)
  - JMFF_PATH, JMFF_ENCODED_PATH, JMFF_SIZE, JMFF_LAST_MODIFIED, JMFF_CHECKSUM_ALGORITHM, JMFF_CHECKSUM (file field enum values)
  - JMWRF_TIMELINE, JMWRF_START_LSN, JMWRF_END_LSN (WAL range field enum values)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - json_parse_manifest_incremental_init
  - json_parse_manifest

## Notes and Other Information
- This is a static callback function used specifically within the manifest parsing infrastructure
- The Last-Modified field value is explicitly freed and unused in the current implementation
- Version and system identifier values trigger immediate finalization for validation purposes
- Other field values are stored in the parse state and processed later when object parsing completes
- The tokentype parameter is accepted for compatibility but not currently used in the implementation
- Error handling is provided for unexpected scalar values through json_manifest_parse_failure
- The function always returns JSON_SUCCESS, with errors handled through the parse failure mechanism