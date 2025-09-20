# manifest_process_file

## Location
[src/backend/backup/basebackup_incremental.c:968-994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L968-L994)

## Overview
A callback function invoked for each file mentioned in the backup manifest to store file path and size information for sanity-checking purposes during incremental backup processing.

## Definition

```c
static void
manifest_process_file(JsonManifestParseContext *context,
					  const char *pathname, size_t size,
					  pg_checksum_type checksum_type,
					  int checksum_length,
					  uint8 *checksum_payload)
```
## Detailed Description
This function is a callback that gets invoked during backup manifest parsing for every file entry encountered. It extracts and stores essential file metadata (path and size) in the IncrementalBackupInfo structure's manifest_files hash table. The function performs memory management by duplicating the pathname string in the appropriate memory context and only processes new entries (avoiding duplicates). While checksum-related parameters are provided, they are not currently used by this implementation, focusing instead on basic file tracking for incremental backup validation.

## Parameters / Member Variables
- : JsonManifestParseContext pointer containing parsing state and private data
- : Path string of the file being processed from the manifest
- : Size of the file in bytes
- : Type of checksum algorithm used (not currently utilized)
- : Length of the checksum data (not currently utilized)  
- : Actual checksum bytes (not currently utilized)

## Dependencies
- Functions called/Symbols referenced:
  - backup_file_insert
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
- Called from (representative examples):
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md) (as callback in manifest parsing)

## Notes and Other Information
- This is a static function local to basebackup_incremental.c
- Part of the incremental backup infrastructure introduced for more efficient backup operations
- The function currently ignores checksum information, focusing only on file existence and size tracking
- Uses hash table storage for efficient file lookup during backup processing
- Memory allocation uses the manifest_files hash table's memory context for proper cleanup