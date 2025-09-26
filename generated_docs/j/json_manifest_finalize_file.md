# json_manifest_finalize_file

## Location
src/common/parse_manifest.c: 649 - 750

## Overview
Performs comprehensive parsing, validation, and processing of file information from JSON manifest data, including pathname decoding, size parsing, and checksum validation before invoking a per-file callback.

## Definition
```c
static void
json_manifest_finalize_file(JsonManifestParseState *parse)
```

## Detailed Description
This function is the core file processing routine for JSON manifest parsing in PostgreSQL's backup system. It handles all aspects of individual file entry processing:

1. **Field Validation**: Ensures required fields (pathname/encoded_pathname and size) are present and mutually exclusive where appropriate
2. **Pathname Processing**: Decodes hex-encoded pathnames when necessary and validates the decoding process
3. **Size Parsing**: Converts string size representation to numeric value with error checking
4. **Checksum Processing**: Parses checksum algorithm and payload, supporting various checksum types
5. **Memory Management**: Properly frees allocated memory after processing
6. **Callback Invocation**: Calls the per-file callback with all processed file information

The function is designed to handle both regular pathnames and hex-encoded pathnames (for filenames containing special characters), and supports various checksum algorithms for data integrity verification.

## Parameters / Member Variables
- `parse`: Pointer to JsonManifestParseState structure containing parsed file information including pathname, encoded_pathname, size, algorithm, and checksum fields

## Dependencies
- Functions called/Symbols referenced:
  - `json_manifest_parse_failure` - error reporting for parsing failures
  - `palloc` - PostgreSQL memory allocation
  - `pfree` - PostgreSQL memory deallocation  
  - `hexdecode_string` - hex string to binary conversion
  - `strtoul` - string to unsigned long conversion
  - `pg_checksum_parse_type` - checksum algorithm type parsing
  - `strlen` - string length calculation
  - `CHECKSUM_TYPE_NONE` - constant for no checksum type
  - `JsonManifestParseState` - parsing state structure
  - `JsonManifestParseContext` - parsing context structure
  - `pg_checksum_type` - checksum type enumeration
- Called from (representative examples):
  - `json_manifest_object_end` - JSON object completion handler
  - Used in `JsonManifestParseIncrementalState` structure

## Notes and Other Information
- This is a static function, only accessible within the parse_manifest.c file
- Handles both regular pathnames and hex-encoded pathnames for special character support
- Implements comprehensive error checking for all file attributes
- Supports multiple checksum algorithms through the `pg_checksum_parse_type` function
- Performs proper memory cleanup after processing to prevent memory leaks
- The function enforces that pathname and encoded_pathname are mutually exclusive
- Checksum validation includes both algorithm validation and payload hex-decoding
- Part of PostgreSQL's backup manifest infrastructure for ensuring backup integrity
- Uses PostgreSQL's internal memory management functions (palloc/pfree) for consistent memory handling