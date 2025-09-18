# sendFileWithContent

## Location
src/backend/backup/basebackup.c: 1073 - 1133

## Overview
 injects a file with specified name and content directly into the output tar stream during base backup operations.

## Definition


## Detailed Description
This function creates a synthetic file entry in the backup tar stream without requiring the file to exist on disk. It constructs appropriate file metadata (including ownership, permissions, and timestamps), writes a proper tar header, and streams the content data. The function handles checksum computation for backup manifest integration and ensures proper tar formatting with padding.

The function is commonly used for backup metadata files like backup_label, tablespace_map, and WAL archive status files (.done files) that need to be included in backups but are generated dynamically rather than read from disk.

Key operations:
1. Initialize checksum computation for the synthetic file
2. Construct a complete stat structure with appropriate metadata
3. Write tar header with file information
4. Stream content data in chunks through the bbsink pipeline
5. Add appropriate tar padding and register file in backup manifest

## Parameters / Member Variables
- : bbsink pipeline for writing backup data
- : Target filename to appear in the tar archive
- : String or binary content to write as file data
- : Length of content data, or -1 to use strlen() for string content
- USAGE:
  /usr/bin/manifest export [-|URL|FILENAME]
  /usr/bin/manifest import -|URL|FILENAME: Backup manifest info for checksum and file registration

## Dependencies
- Functions called/Symbols referenced:
  - pg_checksum_init
  - pg_checksum_update
  - _tarWriteHeader
  - bbsink_archive_contents
  - _tarWritePadding
  - AddFileToBackupManifest
- Called from (representative examples):
  - perform_base_backup (for backup_label, tablespace_map, .done files)

## Notes and Other Information
- Uses current effective user/group IDs and timestamp for synthetic file metadata on Unix systems  
- On Windows, sets uid/gid to 0 since these concepts don't exist
- File permissions are set to pg_file_create_mode for consistency
- Supports both string content (len=-1) and binary data (explicit len)
- Properly integrates with backup manifest system for file verification
- Essential for including dynamically generated metadata files in backups