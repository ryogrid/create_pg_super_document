# manifest_process_version

## Location
[src/backend/backup/basebackup_incremental.c:932-945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L932-L945)

## Overview
A callback function that validates the manifest version compatibility for incremental backup operations, ensuring only supported manifest versions are used.

## Definition
static void manifest_process_version(JsonManifestParseContext *context, int manifest_version)

## Detailed Description
This function serves as a validation callback within PostgreSQL's incremental backup manifest processing system. It checks whether the provided manifest version is compatible with incremental backup operations. Currently, it specifically rejects manifest version 1, which lacks the necessary features to support incremental backups. When an incompatible version is detected, it triggers an error through the context's error callback mechanism, preventing the backup operation from proceeding with an unsupported manifest format.

## Parameters / Member Variables
- context: Pointer to JsonManifestParseContext structure containing parsing state and error handling callbacks
- manifest_version: Integer representing the version number of the backup manifest being processed

## Dependencies
- Functions called/Symbols referenced:
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md) (structure for manifest parsing context)
  - context->error_cb (error callback function from the context)
- Called from (representative examples):
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md) (structure that utilizes this validation callback)

## Notes and Other Information
- This is a static function with internal linkage, used only within the incremental backup module
- The function specifically blocks manifest version 1, which lacks incremental backup support
- Error reporting is handled through the context's callback mechanism rather than direct error throwing
- Part of PostgreSQL's robust version compatibility checking system for backup operations
- Ensures data integrity by preventing incompatible manifest versions from being processed in incremental backup scenarios

## Simplified Source

```c
static void manifest_process_version(JsonManifestParseContext *context, int manifest_version) {
    // Check if manifest version supports incremental backups
    if (manifest_version == 1) {
        // Manifest version 1 lacks incremental backup features
        context->error_cb(context, "backup manifest version 1 does not support incremental backup");
    }
}
```