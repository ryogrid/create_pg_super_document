# parse_xlogrecptr

## Location
[src/common/parse_manifest.c:939-948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L939-L948)

## Overview
A static function that parses PostgreSQL XLogRecPtr values from their standard string representation format (HEX/HEX) into a 64-bit integer.

## Definition
```c
static bool parse_xlogrecptr(XLogRecPtr *result, char *input)
```

## Detailed Description
The `parse_xlogrecptr` function converts PostgreSQL transaction log record pointers (XLogRecPtr) from their canonical string representation to their internal 64-bit integer format. XLogRecPtr values are commonly displayed and stored as two hexadecimal numbers separated by a slash (e.g., "1/A0000000"), where the first part represents the high-order 32 bits and the second part represents the low-order 32 bits.

This function uses `sscanf` to parse both hexadecimal components from the input string, then combines them into a single 64-bit value by shifting the high-order part left by 32 bits and OR-ing it with the low-order part. This reconstruction matches PostgreSQL's internal representation of transaction log positions.

The function is essential for parsing backup manifests that contain WAL (Write-Ahead Logging) range information, allowing the system to understand and validate transaction log positions and ranges.

## Parameters / Member Variables
- `result`: Pointer to XLogRecPtr variable where the parsed 64-bit value will be stored
- `input`: String containing the XLogRecPtr in "HEX/HEX" format (e.g., "1/A0000000")

## Return Value
- Returns `true` if the input string was successfully parsed as a valid XLogRecPtr
- Returns `false` if the input format is invalid or parsing fails

## Dependencies
- Functions called/Symbols referenced:
  - sscanf (standard C library function for formatted input)
  - XLogRecPtr (PostgreSQL type representing transaction log positions)

- Called from (representative examples):
  - [json_manifest_finalize_wal_range](../j/json_manifest_finalize_wal_range.md) (for parsing WAL range boundaries)
  - [JsonManifestParseIncrementalState](../J/JsonManifestParseIncrementalState.md) (structure reference)

## Notes and Other Information
- This is a static function, limiting its scope to the parse_manifest.c compilation unit
- Uses `sscanf` with "%X/%X" format specifier to parse two hexadecimal values separated by a slash
- The reconstruction formula: result = (hi << 32) | lo, where hi and lo are the parsed 32-bit components
- XLogRecPtr represents positions in PostgreSQL's Write-Ahead Log (WAL)
- Used specifically in backup manifest parsing for WAL range validation
- The function expects exactly two hexadecimal numbers separated by a slash
- Does not perform additional validation on the parsed values beyond format checking
- Critical for ensuring backup consistency by validating WAL position ranges