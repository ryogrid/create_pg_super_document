# sendFile

## Location
[src/backend/backup/basebackup.c:1572-1846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L1572-L1846)

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
  - [OpenTransientFile](../O/OpenTransientFile.md), CloseTransientFile
  - [read_file_data_into_buffer](../r/read_file_data_into_buffer.md)
  - [push_to_sink](../p/push_to_sink.md)
  - [_tarWriteHeader](../t/_tarWriteHeader.md), _tarWritePadding
  - [bbsink_archive_contents](../b/bbsink_archive_contents.md)
  - [pg_checksum_init](../p/pg_checksum_init.md), pg_checksum_update
  - [DataChecksumsEnabled](../D/DataChecksumsEnabled.md)
  - [AddFileToBackupManifest](../A/AddFileToBackupManifest.md)
  - [pgstat_report_checksum_failures_in_db](../p/pgstat_report_checksum_failures_in_db.md)
- Called from (representative examples):
  - [sendDir](sendDir.md)
  - [perform_base_backup](../p/perform_base_backup.md)

## Notes and Other Information
- Returns true if file was successfully sent, false if missing_ok=true and file doesn't exist
- For incremental backups, creates a special header with magic number and block list
- Handles concurrent file truncation by padding with zeros (WAL replay will fix)
- Verifies checksums only for relation files when checksums are enabled cluster-wide
- Updates cumulative checksum failure statistics and reports to stats system
- Maintains proper TAR block alignment with padding as required by TAR format
- Located in src/backend/backup/basebackup.c:1572-1846

## Simplified Source

```c
// Simplified version of sendFile (very complex function condensed)
static bool sendFile(bbsink *sink, const char *readfilename, const char *tarfilename,
                    struct stat *statbuf, bool missing_ok, Oid dboid, Oid spcoid,
                    RelFileNumber relfilenumber, unsigned segno,
                    backup_manifest_info *manifest, unsigned num_incremental_blocks,
                    BlockNumber *incremental_blocks, unsigned truncation_block_length) {
    int fd;
    BlockNumber blkno = 0;
    int checksum_failures = 0;
    off_t cnt;
    pgoff_t bytes_done = 0;
    bool verify_checksum = false;
    pg_checksum_context checksum_ctx;
    int ibindex = 0;

    // Initialize checksum context
    if (pg_checksum_init(&checksum_ctx, manifest->checksum_type) < 0)
        elog(ERROR, "could not initialize checksum of file \"%s\"", readfilename);

    // Open file for reading
    fd = OpenTransientFile(readfilename, O_RDONLY | PG_BINARY);
    if (fd < 0) {
        if (errno == ENOENT && missing_ok)
            return false;
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not open file \"%s\": %m", readfilename)));
    }

    // Write TAR header
    _tarWriteHeader(sink, tarfilename, NULL, statbuf, false);

    // Enable checksum verification for relation files
    if (!noverify_checksums && DataChecksumsEnabled() &&
        RelFileNumberIsValid(relfilenumber))
        verify_checksum = true;

    // Handle incremental backup header
    if (incremental_blocks != NULL) {
        // Write incremental file header with magic number and block list
        unsigned magic = INCREMENTAL_MAGIC;
        size_t header_bytes_done = 0;

        push_to_sink(sink, &checksum_ctx, &header_bytes_done,
                     &magic, sizeof(magic));
        push_to_sink(sink, &checksum_ctx, &header_bytes_done,
                     &num_incremental_blocks, sizeof(num_incremental_blocks));
        push_to_sink(sink, &checksum_ctx, &header_bytes_done,
                     &truncation_block_length, sizeof(truncation_block_length));
        push_to_sink(sink, &checksum_ctx, &header_bytes_done,
                     incremental_blocks, sizeof(BlockNumber) * num_incremental_blocks);

        // Add padding and flush header
        if ((num_incremental_blocks > 0) && (header_bytes_done % BLCKSZ != 0)) {
            size_t paddinglen = (BLCKSZ - (header_bytes_done % BLCKSZ));
            char padding[BLCKSZ];
            memset(padding, 0, paddinglen);
            push_to_sink(sink, &checksum_ctx, &header_bytes_done, padding, paddinglen);
        }

        if (header_bytes_done > 0) {
            bbsink_archive_contents(sink, header_bytes_done);
            pg_checksum_update(&checksum_ctx, (uint8 *) sink->bbs_buffer, header_bytes_done);
        }

        bytes_done += sizeof(magic) + sizeof(num_incremental_blocks) +
                      sizeof(truncation_block_length) + sizeof(BlockNumber) * num_incremental_blocks;
    }

    // Main file reading loop
    while (1) {
        if (incremental_blocks == NULL) {
            // Full file mode: read sequentially
            size_t remaining = statbuf->st_size - bytes_done;
            if (bytes_done >= statbuf->st_size)
                break;

            cnt = read_file_data_into_buffer(sink, readfilename, fd, bytes_done, remaining,
                                           blkno + segno * RELSEG_SIZE, verify_checksum,
                                           &checksum_failures);
        } else {
            // Incremental mode: read specific blocks
            if (ibindex >= num_incremental_blocks)
                break;

            BlockNumber relative_blkno = incremental_blocks[ibindex++];
            cnt = read_file_data_into_buffer(sink, readfilename, fd,
                                           relative_blkno * BLCKSZ, BLCKSZ,
                                           relative_blkno + segno * RELSEG_SIZE,
                                           verify_checksum, &checksum_failures);

            if (cnt < BLCKSZ) // File truncation detected
                break;
        }

        // Validate block alignment for checksum verification
        if (verify_checksum && (cnt % BLCKSZ != 0)) {
            ereport(WARNING, (errmsg("could not verify checksum in file \"%s\"",
                                   readfilename)));
            verify_checksum = false;
        }

        if (cnt == 0) // End of file
            break;

        // Update counters and archive data
        blkno += cnt / BLCKSZ;
        bytes_done += cnt;
        bbsink_archive_contents(sink, cnt);
        pg_checksum_update(&checksum_ctx, (uint8 *) sink->bbs_buffer, cnt);
    }

    // Pad truncated files with zeros
    while (bytes_done < statbuf->st_size) {
        size_t remaining = statbuf->st_size - bytes_done;
        size_t nbytes = Min(sink->bbs_buffer_length, remaining);

        MemSet(sink->bbs_buffer, 0, nbytes);
        pg_checksum_update(&checksum_ctx, (uint8 *) sink->bbs_buffer, nbytes);
        bbsink_archive_contents(sink, nbytes);
        bytes_done += nbytes;
    }

    // Add TAR padding and cleanup
    _tarWritePadding(sink, bytes_done);
    CloseTransientFile(fd);

    // Report checksum failures
    if (checksum_failures > 1) {
        ereport(WARNING, (errmsg_plural("file \"%s\" has a total of %d checksum verification failure",
                                       "file \"%s\" has a total of %d checksum verification failures",
                                       checksum_failures, readfilename, checksum_failures)));
        pgstat_report_checksum_failures_in_db(dboid, checksum_failures);
    }

    total_checksum_failures += checksum_failures;
    AddFileToBackupManifest(manifest, spcoid, tarfilename, statbuf->st_size,
                           (pg_time_t) statbuf->st_mtime, &checksum_ctx);

    return true;
}
```

Key simplifications made:
- Condensed the complex file reading logic while preserving essential flow
- Maintained incremental backup header generation and processing
- Preserved checksum verification and error handling
- Kept TAR format compliance with headers and padding
- Maintained file truncation handling and recovery logic
- Preserved backup manifest integration and statistics reporting
- Note: Some detailed error handling condensed but core functionality intact