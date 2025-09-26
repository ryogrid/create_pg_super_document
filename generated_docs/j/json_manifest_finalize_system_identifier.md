# json_manifest_finalize_system_identifier

## Location
[src/common/parse_manifest.c:624-648](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L624-L648)

## Overview
Performs additional parsing and validation of the system identifier field from manifest data and invokes a callback to notify the caller about the parsed system identifier.

## Definition
```c
static void
json_manifest_finalize_system_identifier(JsonManifestParseState *parse)
```

## Detailed Description
This function processes the system identifier field from a JSON manifest file after the complete JSON object has been parsed. The system identifier is a unique 64-bit value that identifies a PostgreSQL database system instance. The function:

1. **System Identifier Parsing**: Converts the string representation of the system identifier to a 64-bit unsigned integer using `strtou64()`
2. **Validation**: Ensures the system identifier is a valid integer value
3. **Callback Invocation**: Calls the system identifier callback function to notify the caller about the parsed value

This is part of PostgreSQL's backup and restore infrastructure, where system identifiers are used to ensure that WAL files and backup data belong to the correct database system instance.

## Parameters / Member Variables
- `parse`: Pointer to JsonManifestParseState structure containing the current parsing state, including the manifest_system_identifier string field and parsing context

## Dependencies
- Functions called/Symbols referenced:
  - `strtou64` - converts string to 64-bit unsigned integer
  - `json_manifest_parse_failure` - handles parsing error reporting
  - `JsonManifestParseState` - parsing state structure
  - `JsonManifestParseContext` - parsing context structure
- Called from (representative examples):
  - `json_manifest_scalar` - JSON scalar value processing function
  - Used in `JsonManifestParseIncrementalState` structure

## Notes and Other Information
- This is a static function, only accessible within the parse_manifest.c file
- Requires that the `manifest_system_identifier` field is not NULL (checked via Assert)
- Uses PostgreSQL's internal `strtou64()` function for robust unsigned integer parsing with error detection
- The system identifier is crucial for ensuring data integrity in backup and restore operations
- Any parsing errors result in a failure callback being invoked to handle the error appropriately
- Part of the incremental JSON parsing framework for handling large manifest files efficiently