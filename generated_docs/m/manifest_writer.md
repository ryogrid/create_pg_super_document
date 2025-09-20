# manifest_writer

## Location
[src/bin/pg_combinebackup/write_manifest.c:27-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/write_manifest.c#L27-L47)

## Overview
The  struct manages the creation and writing of PostgreSQL backup manifest files, maintaining state for incremental JSON generation and checksumming.

## Definition

```c
struct manifest_writer
{
	char		pathname[MAXPGPATH];
	int			fd;
	StringInfoData buf;
	bool		first_file;
	bool		still_checksumming;
	pg_checksum_context manifest_ctx;
};
```
## Detailed Description
The  struct is used by PostgreSQL's  utility to incrementally build backup manifest files in JSON format. It maintains both the file I/O state and the in-memory buffer for constructing the manifest content. The struct supports efficient streaming output by buffering manifest entries and flushing them when the buffer reaches a threshold (128KB). It also maintains a checksum context for computing the manifest's integrity hash.

## Parameters / Member Variables
- : Full file path where the backup_manifest file will be written
- : File descriptor for the opened manifest file (-1 when not yet opened)  
- : StringInfoData buffer for accumulating JSON manifest content before writing to disk
- : Boolean flag tracking whether the next file entry is the first one (affects JSON formatting)
- : Boolean flag indicating whether checksum computation is still active
- : Checksum context for computing SHA256 hash of the entire manifest content

## Dependencies
- Functions called/Symbols referenced:
  -  (for manifest integrity verification)
  -  (for proper JSON string encoding)
  -  (for encoding binary data as hexadecimal)
  -  (for writing buffered content to disk)

- Called from (representative examples):
  -  (constructor function at write_manifest.c:50)
  -  (adds file entries at write_manifest.c:76)
  -  (completes the manifest at write_manifest.c:142)
  -  (main processing loop at pg_combinebackup.c:830)

## Notes and Other Information
- Used exclusively within the pg_combinebackup utility for creating backup manifest files
- The manifest format follows PostgreSQL Backup Manifest Version 2 specification
- Buffer management uses a 128KB threshold to balance memory usage and I/O efficiency
- Supports both UTF-8 and hex-encoded file paths for international filename compatibility
- The struct is allocated and managed through create_manifest_writer() and related functions
- Located in src/bin/pg_combinebackup/write_manifest.c:27-35