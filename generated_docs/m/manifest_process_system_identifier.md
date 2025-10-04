# manifest_process_system_identifier

## Location
[src/backend/backup/basebackup_incremental.c:946-967](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L946-L967)

## Overview
A callback function that validates the system identifier in a backup manifest against the current database server's system identifier to ensure backup compatibility.

## Definition
static void manifest_process_system_identifier(JsonManifestParseContext *context, uint64 manifest_system_identifier)

## Detailed Description
This function performs a critical security and consistency check during incremental backup manifest processing. It compares the system identifier stored in the backup manifest with the current database server's system identifier. The system identifier is a unique value that distinguishes one PostgreSQL database cluster from another. If these identifiers don't match, it indicates that the backup was created from a different database cluster, which would make incremental backup operations invalid and potentially dangerous. When a mismatch is detected, the function triggers an error through the context's callback mechanism, preventing the potentially corrupted incremental backup operation from proceeding.

## Parameters / Member Variables
- context: Pointer to JsonManifestParseContext structure containing parsing state and error handling callbacks
- manifest_system_identifier: The system identifier value extracted from the backup manifest being processed

## Dependencies
- Functions called/Symbols referenced:
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md) (structure for manifest parsing context)
  - [GetSystemIdentifier](../G/GetSystemIdentifier.md) (function to retrieve the current database system's unique identifier)
  - context->error_cb (error callback function from the context)
- Called from (representative examples):
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md) (structure that utilizes this validation callback)

## Notes and Other Information
- This is a static function with internal linkage, used only within the incremental backup module
- System identifier validation is crucial for preventing data corruption in incremental backup scenarios
- The error message includes both identifiers (manifest and current system) for debugging purposes
- Part of PostgreSQL's comprehensive backup integrity checking system
- Prevents cross-cluster backup operations which could lead to serious data consistency issues
- The system identifier is a 64-bit value that uniquely identifies each PostgreSQL database cluster

## Simplified Source

```c
static void
manifest_process_system_identifier(JsonManifestParseContext *context,
                                  uint64 manifest_system_identifier)
{
    // Get current system's identifier
    uint64 system_identifier = GetSystemIdentifier();

    // Check if manifest matches current system
    if (manifest_system_identifier != system_identifier) {
        context->error_cb(context,
                         "system identifier mismatch: manifest=%llu, current=%llu",
                         (unsigned long long) manifest_system_identifier,
                         (unsigned long long) system_identifier);
    }
}
```