# json_manifest_parse_failure

## Location
[src/common/parse_manifest.c:889-899](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L889-L899)

## Overview
A static error reporting function used during JSON backup manifest parsing to handle parse failures by invoking the appropriate error callback with a formatted error message.

## Definition
```c
static void json_manifest_parse_failure(JsonManifestParseContext *context, char *msg)
```

## Detailed Description
The `json_manifest_parse_failure` function is a centralized error reporting mechanism for the JSON backup manifest parsing system. It serves as a uniform way to handle parse errors that occur during manifest processing, whether due to malformed JSON, unexpected data structures, or validation failures. The function delegates the actual error handling to a callback function stored in the parsing context, allowing for flexible error handling strategies depending on the calling context.

This function is designed to handle fairly low-level parsing failures that typically indicate either deliberate construction of malformed manifests or bugs in the server's manifest generation logic. It provides a consistent interface for error reporting across all manifest parsing operations.

## Parameters / Member Variables
- `context`: Pointer to JsonManifestParseContext containing the parsing state and error callback function
- `msg`: A short descriptive string indicating the specific nature of the parsing problem

## Dependencies
- Functions called/Symbols referenced:
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md) (struct type)
  - context->error_cb (callback function pointer)

- Called from (representative examples):
  - [json_parse_manifest_incremental_chunk](json_parse_manifest_incremental_chunk.md)
  - [json_parse_manifest](json_parse_manifest.md)
  - [json_manifest_object_start](json_manifest_object_start.md)
  - [json_manifest_object_end](json_manifest_object_end.md)
  - [json_manifest_array_start](json_manifest_array_start.md)
  - [json_manifest_array_end](json_manifest_array_end.md)
  - [json_manifest_object_field_start](json_manifest_object_field_start.md)
  - [json_manifest_scalar](json_manifest_scalar.md)
  - [json_manifest_finalize_version](json_manifest_finalize_version.md)
  - [json_manifest_finalize_system_identifier](json_manifest_finalize_system_identifier.md)
  - [json_manifest_finalize_file](json_manifest_finalize_file.md)
  - [json_manifest_finalize_wal_range](json_manifest_finalize_wal_range.md)
  - [verify_manifest_checksum](../v/verify_manifest_checksum.md)

## Notes and Other Information
- This is a static function, meaning it's only visible within the parse_manifest.c compilation unit
- The function formats the error message with "could not parse backup manifest: %s" prefix
- Error handling strategy is determined by the error callback function stored in the context
- Used extensively throughout the manifest parsing pipeline for consistent error reporting
- The function is designed for low-level parse failures rather than high-level validation errors

## Simplified Source

```c
static void
json_manifest_parse_failure(JsonManifestParseContext *context, char *msg)
{
    context->error_cb(context, "could not parse backup manifest: %s", msg);
}
```