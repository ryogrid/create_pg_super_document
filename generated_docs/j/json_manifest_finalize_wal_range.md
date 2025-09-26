# json_manifest_finalize_wal_range

## Location
[src/common/parse_manifest.c:751-811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L751-L811)

## Overview
Performs parsing, validation, and processing of WAL (Write-Ahead Log) range information from JSON manifest data and invokes a callback to notify the caller about the processed WAL range details.

## Definition
```c
static void
json_manifest_finalize_wal_range(JsonManifestParseState *parse)
```

## Detailed Description
This function handles the finalization of WAL range entries in PostgreSQL's backup manifest system. WAL ranges specify the timeline and LSN (Log Sequence Number) boundaries for WAL data included in a backup. The function:

1. **Field Validation**: Ensures all required fields (timeline, start_lsn, end_lsn) are present in the parsed data
2. **Timeline Parsing**: Converts the string timeline representation to a TimeLineID using `strtoul()`
3. **LSN Parsing**: Processes start and end LSN values using `parse_xlogrecptr()` to convert string representations to XLogRecPtr structures
4. **Error Handling**: Validates that all conversions succeed and reports parsing failures appropriately
5. **Callback Invocation**: Calls the per-WAL-range callback with the parsed timeline and LSN information
6. **Memory Management**: Frees allocated string memory after processing

WAL ranges are critical for backup consistency, defining exactly which portions of the transaction log are included with a backup set.

## Parameters / Member Variables
- `parse`: Pointer to JsonManifestParseState structure containing parsed WAL range information including timeline, start_lsn, and end_lsn string fields

## Dependencies
- Functions called/Symbols referenced:
  - `json_manifest_parse_failure` - error reporting for parsing failures
  - `strtoul` - string to unsigned long conversion for timeline
  - `parse_xlogrecptr` - converts string to XLogRecPtr for LSN values
  - `pfree` - PostgreSQL memory deallocation
  - `JsonManifestParseState` - parsing state structure
  - `JsonManifestParseContext` - parsing context structure
  - `TimeLineID` - timeline identifier type
  - `XLogRecPtr` - WAL record pointer type
- Called from (representative examples):
  - `json_manifest_object_end` - JSON object completion handler
  - Used in `JsonManifestParseIncrementalState` structure

## Notes and Other Information
- This is a static function, only accessible within the parse_manifest.c file
- All three fields (timeline, start_lsn, end_lsn) are mandatory for WAL range entries
- Timeline IDs are parsed as unsigned integers representing PostgreSQL timeline numbers
- LSN parsing uses PostgreSQL's specialized `parse_xlogrecptr()` function for proper XLogRecPtr format handling
- The function performs comprehensive error checking on all parsed values
- Memory cleanup is performed after successful processing to prevent memory leaks
- WAL ranges are essential for point-in-time recovery and backup consistency verification
- Part of PostgreSQL's backup manifest infrastructure ensuring WAL data integrity and completeness
- Uses PostgreSQL's internal memory management (pfree) for consistent memory handling