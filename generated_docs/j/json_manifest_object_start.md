# json_manifest_object_start

## Location
[src/common/parse_manifest.c:276-316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L276-L316)

## Overview
JSON semantic action handler that processes the start of objects in backup manifest JSON documents, managing parser state transitions.

## Definition

```c
static JsonParseErrorType
json_manifest_object_start(void *state)
```
## Detailed Description
This function serves as a semantic action callback for the JSON parser, invoked whenever an opening brace '{' is encountered in the manifest JSON. It manages the parser's finite state machine by transitioning between different parsing states based on the current context.

The function handles three main object types in manifest JSON: the top-level document object, individual file objects within the files array, and WAL range objects within the WAL ranges array. For file and WAL range objects, it initializes the relevant parsing fields to NULL in preparation for processing the object's contents.

## Parameters / Member Variables
- : Void pointer to JsonManifestParseState containing current parser state and field values

## Dependencies
- Functions called/Symbols referenced:
  - [json_manifest_parse_failure](json_manifest_parse_failure.md)
  - JSON_SUCCESS
  - JM_EXPECT_TOPLEVEL_START
  - JM_EXPECT_TOPLEVEL_FIELD
  - JM_EXPECT_FILES_NEXT
  - JM_EXPECT_THIS_FILE_FIELD
  - JM_EXPECT_WAL_RANGES_NEXT
  - JM_EXPECT_THIS_WAL_RANGE_FIELD
- Called from (representative examples):
  - [json_parse_manifest_incremental_init](json_parse_manifest_incremental_init.md) (src/common/parse_manifest.c:145)
  - [json_parse_manifest](json_parse_manifest.md) (src/common/parse_manifest.c:245)

## Notes and Other Information
- Returns JSON_SUCCESS on successful state transition or triggers parse failure for unexpected objects
- Resets file-related fields (pathname, encoded_pathname, size, algorithm, checksum) when entering file object parsing
- Resets WAL range fields (timeline, start_lsn, end_lsn) when entering WAL range object parsing
- Part of the semantic action handler suite for JSON manifest parsing
- Static function used internally by the manifest parsing system
- Critical for maintaining correct parser state machine behavior during manifest processing