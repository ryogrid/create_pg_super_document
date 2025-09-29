# sendFileWithContent

## Location
[src/backend/backup/basebackup.c:1073-1133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L1073-L1133)

## Overview
 injects a file with specified name and content directly into the output tar stream during base backup operations.

## Definition

```c
struct stat statbuf;
```
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
  - [pg_checksum_init](../p/pg_checksum_init.md)
  - [pg_checksum_update](../p/pg_checksum_update.md)
  - [_tarWriteHeader](../t/_tarWriteHeader.md)
  - [bbsink_archive_contents](../b/bbsink_archive_contents.md)
  - [_tarWritePadding](../t/_tarWritePadding.md)
  - [AddFileToBackupManifest](../A/AddFileToBackupManifest.md)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md) (for backup_label, tablespace_map, .done files)

## Notes and Other Information
- Uses current effective user/group IDs and timestamp for synthetic file metadata on Unix systems  
- On Windows, sets uid/gid to 0 since these concepts don't exist
- File permissions are set to pg_file_create_mode for consistency
- Supports both string content (len=-1) and binary data (explicit len)
- Properly integrates with backup manifest system for file verification
- Essential for including dynamically generated metadata files in backups

## Simplified Source

```c
// Simplified version of sendFileWithContent
static void
sendFileWithContent(bbsink *sink, const char *filename, const char *content,
                   int len, backup_manifest_info *manifest)
{
    struct stat statbuf;
    int bytes_done = 0;
    pg_checksum_context checksum_ctx;

    // Initialize checksum computation
    if (pg_checksum_init(&checksum_ctx, manifest->checksum_type) < 0)
        elog(ERROR, "could not initialize checksum of file \"%s\"", filename);

    // Handle string content length
    if (len < 0)
        len = strlen(content);

    // Set up file metadata for tar entry
    statbuf.st_uid = geteuid();    // Use effective user ID (0 on Windows)
    statbuf.st_gid = getegid();    // Use effective group ID (0 on Windows)
    statbuf.st_mtime = time(NULL); // Current timestamp
    statbuf.st_mode = pg_file_create_mode;
    statbuf.st_size = len;

    // Write tar header for the synthetic file
    _tarWriteHeader(sink, filename, NULL, &statbuf, false);

    // Update checksum with content data
    if (pg_checksum_update(&checksum_ctx, (uint8 *) content, len) < 0)
        elog(ERROR, "could not update checksum of file \"%s\"", filename);

    // Stream content data in chunks
    while (bytes_done < len) {
        size_t remaining = len - bytes_done;
        size_t chunk_size = Min(sink->bbs_buffer_length, remaining);

        memcpy(sink->bbs_buffer, content, chunk_size);
        bbsink_archive_contents(sink, chunk_size);

        bytes_done += chunk_size;
        content += chunk_size;
    }

    // Add tar padding and register in manifest
    _tarWritePadding(sink, len);
    AddFileToBackupManifest(manifest, InvalidOid, filename, len,
                           (pg_time_t) statbuf.st_mtime, &checksum_ctx);
}
```

Key simplifications made:
- Removed platform-specific #ifdef WIN32 conditionals for clarity
- Consolidated variable declarations and initialization
- Added descriptive comments for each major operation
- Simplified the chunked data streaming loop structure
- Removed detailed error context while preserving essential error checking
- Used more descriptive variable names (chunk_size instead of nbytes)
- Streamlined the overall flow to highlight the core algorithm