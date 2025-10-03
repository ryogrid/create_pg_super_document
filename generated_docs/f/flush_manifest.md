# flush_manifest

## Location
[src/bin/pg_combinebackup/write_manifest.c:244-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/write_manifest.c#L244-L279)

## Overview
Flushes buffered backup manifest data to disk, creating the manifest file on first call and updating the running checksum.

## Definition

```c
static void
flush_manifest(manifest_writer *mwriter)
```
## Detailed Description
This static function writes the accumulated manifest data from the in-memory buffer to the backup manifest file on disk. On the first call, it creates the manifest file with exclusive creation flags to prevent overwriting existing files. Subsequent calls append additional data to the already-open file. The function also maintains a running SHA256 checksum of the written data when checksumming is active, and resets the buffer after successful writes.

Key operations include:
- File creation on first flush with O_CREAT | O_EXCL | O_WRONLY flags
- Writing complete buffer contents to disk with error handling
- Updating the running SHA256 checksum of manifest content
- Clearing the buffer after successful write operations
- Comprehensive error reporting for file operations

## Parameters / Member Variables
- `*mwriter`: Manifest writer structure containing the buffer data and file descriptor
## Dependencies
- Functions called/Symbols referenced:
  - [manifest_writer](../m/manifest_writer.md) (structure type)
  - open (file opening system call)
  - PG_BINARY (binary file mode flag)
  - ssize_t (signed size type)
  - write (file writing system call)
  - [pg_checksum_update](../p/pg_checksum_update.md) (checksum updating)
  - [resetStringInfo](../r/resetStringInfo.md) (buffer clearing)
- Called from (representative examples):
  - [add_file_to_manifest](../a/add_file_to_manifest.md) (in src/bin/pg_combinebackup/write_manifest.c:117)
  - [add_file_to_manifest](../a/add_file_to_manifest.md) (in src/bin/pg_combinebackup/write_manifest.c:135)
  - [finalize_manifest](finalize_manifest.md) (in src/bin/pg_combinebackup/write_manifest.c:168)
  - [finalize_manifest](finalize_manifest.md) (in src/bin/pg_combinebackup/write_manifest.c:183)

## Notes and Other Information
- This is a static function, only callable within the write_manifest.c file
- Uses exclusive creation (O_EXCL) to prevent accidental overwriting of existing manifest files
- Performs complete error checking for file operations with descriptive error messages
- The still_checksumming flag controls whether written data is included in checksum calculation
- Buffer is automatically reset after each successful flush to prevent memory accumulation
- File descriptor remains open between flushes for efficiency
- Uses pg_file_create_mode for appropriate file permissions on creation
- Handles partial write scenarios by reporting exactly how many bytes were written