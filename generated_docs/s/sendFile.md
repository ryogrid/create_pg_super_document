# sendFile

## Location
src/backend/backup/basebackup.c: 1572 - 1846

## Overview
sendFile writes a TAR header and sends the contents of a specified file to the backup stream, with support for both full and incremental backups, checksum verification, and comprehensive error handling.

## Definition
```c
static bool sendFile(bbsink *sink, const char *readfilename, const char *tarfilename,
                    struct stat *statbuf, bool missing_ok, Oid dboid, Oid spcoid,
                    RelFileNumber relfilenumber, unsigned segno,
                    backup_manifest_info *manifest, unsigned num_incremental_blocks,
                    BlockNumber *incremental_blocks, unsigned truncation_block_length)
```

## Detailed Description
This function is responsible for transferring individual files during PostgreSQL base backup operations. It supports both full file transfers and incremental backups where only specific blocks are sent. The function creates appropriate TAR headers, handles file I/O with proper error checking, performs checksum verification for relation files, and manages incremental backup metadata. It gracefully handles concurrent file modifications during backup by padding truncated files with zeros, which will be corrected during WAL replay.

Key features include:
- Support for both full and incremental file backup modes
- Checksum verification for relation files when enabled
- Handling of concurrent file truncation during backup
- TAR format compliance with proper headers and padding
- Integration with backup manifest system
- Error reporting for checksum failures

## Parameters / Member Variables
- `sink`: bbsink object representing the backup destination stream
- `readfilename`: File system path of the source file to read
- `tarfilename`: Name to use for the file in the TAR archive
- `statbuf`: Pointer to stat structure containing file metadata
- `missing_ok`: Boolean flag - if true, missing files don't cause errors
- `dboid`: Database OID for checksum failure reporting (InvalidOid if not applicable)
- `spcoid`: Tablespace OID for manifest tracking
- `relfilenumber`: Relation file number for checksum verification
- `segno`: Segment number within the relation file
- `manifest`: Pointer to backup manifest information structure
- `num_incremental_blocks`: Number of blocks to send for incremental backups
- `incremental_blocks`: Array of block numbers to include for incremental backups (NULL for full backup)
- `truncation_block_length`: Block length for handling file truncation

## Dependencies
- Functions called/Symbols referenced:
  - OpenTransientFile, CloseTransientFile
  - read_file_data_into_buffer
  - push_to_sink
  - _tarWriteHeader, _tarWritePadding
  - bbsink_archive_contents
  - pg_checksum_init, pg_checksum_update
  - DataChecksumsEnabled
  - AddFileToBackupManifest
  - pgstat_report_checksum_failures_in_db
- Called from (representative examples):
  - sendDir
  - perform_base_backup

## Notes and Other Information
- Returns true if file was successfully sent, false if missing_ok=true and file doesn't exist
- For incremental backups, creates a special header with magic number and block list
- Handles concurrent file truncation by padding with zeros (WAL replay will fix)
- Verifies checksums only for relation files when checksums are enabled cluster-wide
- Updates cumulative checksum failure statistics and reports to stats system
- Maintains proper TAR block alignment with padding as required by TAR format
- Located in src/backend/backup/basebackup.c:1572-1846