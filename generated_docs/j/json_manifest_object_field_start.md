# json_manifest_object_field_start

## Location
[src/common/parse_manifest.c:401-516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L401-L516)

## Overview
Handles the start of object fields during PostgreSQL backup manifest parsing, identifying and validating field names at different levels of the manifest structure.

## Definition
```c
static JsonParseErrorType json_manifest_object_field_start(void *state, char *fname, bool isnull)
```

## Detailed Description
This callback function is invoked when the JSON parser encounters the start of an object field in a backup manifest. It implements a comprehensive state machine that recognizes and validates field names at three different levels: top-level manifest fields ("PostgreSQL-Backup-Manifest-Version", "System-Identifier", "Files", "WAL-Ranges", "Manifest-Checksum"), individual file object fields ("Path", "Encoded-Path", "Size", "Last-Modified", "Checksum-Algorithm", "Checksum"), and WAL range object fields ("Timeline", "Start-LSN", "End-LSN"). The function sets appropriate parsing states based on the recognized field and stores field type information for subsequent value processing.

## Parameters / Member Variables
- `state`: A void pointer to the JsonManifestParseState structure containing the current parsing context and state information
- `fname`: The field name string that was encountered in the JSON document
- `isnull`: Boolean indicating whether the field value is null (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [json_manifest_parse_failure](json_manifest_parse_failure.md)
  - [pfree](../p/pfree.md)
  - strcmp
  - JsonManifestParseState (struct)
  - JM_EXPECT_TOPLEVEL_FIELD (enum value)
  - JM_EXPECT_VERSION_VALUE (enum value)
  - JM_EXPECT_SYSTEM_IDENTIFIER_VALUE (enum value)
  - JM_EXPECT_FILES_START (enum value)
  - JM_EXPECT_WAL_RANGES_START (enum value)
  - JM_EXPECT_MANIFEST_CHECKSUM_VALUE (enum value)
  - JM_EXPECT_THIS_FILE_FIELD (enum value)
  - JM_EXPECT_THIS_FILE_VALUE (enum value)
  - JM_EXPECT_THIS_WAL_RANGE_FIELD (enum value)
  - JM_EXPECT_THIS_WAL_RANGE_VALUE (enum value)
  - JMFF_PATH, JMFF_ENCODED_PATH, JMFF_SIZE, JMFF_LAST_MODIFIED, JMFF_CHECKSUM_ALGORITHM, JMFF_CHECKSUM (file field enum values)
  - JMWRF_TIMELINE, JMWRF_START_LSN, JMWRF_END_LSN (WAL range field enum values)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - [json_parse_manifest_incremental_init](json_parse_manifest_incremental_init.md)
  - [json_parse_manifest](json_parse_manifest.md)

## Notes and Other Information
- This is a static callback function used specifically within the manifest parsing infrastructure
- The function enforces that "PostgreSQL-Backup-Manifest-Version" must be the first field encountered
- Memory management is handled by calling pfree() on the field name string
- Comprehensive error handling for unrecognized fields at each level through json_manifest_parse_failure
- The function always returns JSON_SUCCESS, with errors handled through the parse failure mechanism
- The isnull parameter is accepted for compatibility but not currently used in the implementation

## Simplified Source

```c
static JsonParseErrorType json_manifest_object_field_start(void *state, char *fname, bool isnull) {
    JsonManifestParseState *parse = state;

    switch (parse->state) {
        case JM_EXPECT_TOPLEVEL_FIELD:
            // Handle top-level manifest fields
            if (!parse->saw_version_field) {
                // Version must be first field
                if (strcmp(fname, "PostgreSQL-Backup-Manifest-Version") != 0)
                    json_manifest_parse_failure(parse->context, "expected version indicator");
                parse->state = JM_EXPECT_VERSION_VALUE;
                parse->saw_version_field = true;
            } else if (strcmp(fname, "System-Identifier") == 0) {
                parse->state = JM_EXPECT_SYSTEM_IDENTIFIER_VALUE;
            } else if (strcmp(fname, "Files") == 0) {
                parse->state = JM_EXPECT_FILES_START;
            } else if (strcmp(fname, "WAL-Ranges") == 0) {
                parse->state = JM_EXPECT_WAL_RANGES_START;
            } else if (strcmp(fname, "Manifest-Checksum") == 0) {
                parse->state = JM_EXPECT_MANIFEST_CHECKSUM_VALUE;
            } else {
                json_manifest_parse_failure(parse->context, "unrecognized top-level field");
            }
            break;

        case JM_EXPECT_THIS_FILE_FIELD:
            // Handle file object fields
            if (strcmp(fname, "Path") == 0)
                parse->file_field = JMFF_PATH;
            else if (strcmp(fname, "Encoded-Path") == 0)
                parse->file_field = JMFF_ENCODED_PATH;
            else if (strcmp(fname, "Size") == 0)
                parse->file_field = JMFF_SIZE;
            else if (strcmp(fname, "Last-Modified") == 0)
                parse->file_field = JMFF_LAST_MODIFIED;
            else if (strcmp(fname, "Checksum-Algorithm") == 0)
                parse->file_field = JMFF_CHECKSUM_ALGORITHM;
            else if (strcmp(fname, "Checksum") == 0)
                parse->file_field = JMFF_CHECKSUM;
            else
                json_manifest_parse_failure(parse->context, "unexpected file field");
            parse->state = JM_EXPECT_THIS_FILE_VALUE;
            break;

        case JM_EXPECT_THIS_WAL_RANGE_FIELD:
            // Handle WAL range object fields
            if (strcmp(fname, "Timeline") == 0)
                parse->wal_range_field = JMWRF_TIMELINE;
            else if (strcmp(fname, "Start-LSN") == 0)
                parse->wal_range_field = JMWRF_START_LSN;
            else if (strcmp(fname, "End-LSN") == 0)
                parse->wal_range_field = JMWRF_END_LSN;
            else
                json_manifest_parse_failure(parse->context, "unexpected WAL range field");
            parse->state = JM_EXPECT_THIS_WAL_RANGE_VALUE;
            break;

        default:
            json_manifest_parse_failure(parse->context, "unexpected object field");
            break;
    }

    pfree(fname);
    return JSON_SUCCESS;
}
```