# verify_file_checksum

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:851-951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L851-L951)

## Overview
Verifies the checksum of a single backup file by reading the file contents and comparing the computed checksum against the expected checksum from the manifest.

## Definition
```c
static void verify_file_checksum(verifier_context *context, manifest_file *m,
                                char *fullpath, uint8 *buffer)
```

## Detailed Description
This function performs the low-level checksum verification for an individual file in the backup. It opens the target file, initializes a checksum context with the appropriate checksum algorithm, reads the file in chunks while incrementally updating the checksum, finalizes the checksum computation, and compares the result against the expected checksum stored in the manifest.

The function includes comprehensive error handling for file operations, checksum computation failures, and data integrity issues. It performs additional validation by ensuring the number of bytes read matches the expected file size from the manifest. Progress reporting is integrated during the file reading process to provide feedback on verification progress.

The verification process includes multiple validation checkpoints:
1. File accessibility and opening
2. Checksum algorithm initialization 
3. Successful file reading and checksum updating
4. File size consistency
5. Checksum length and value matching

## Parameters / Member Variables
- `context`: Pointer to verifier_context structure containing error reporting and verification state
- `m`: Pointer to manifest_file structure containing file metadata, expected size, checksum type, and checksum payload
- `fullpath`: Complete filesystem path to the file being verified
- `buffer`: Pre-allocated buffer for reading file chunks (should be READ_CHUNK_SIZE bytes)

## Dependencies
- Functions called/Symbols referenced:
  - open
  - [pg_checksum_init](../p/pg_checksum_init.md)
  - read
  - [pg_checksum_update](../p/pg_checksum_update.md)
  - close
  - [pg_checksum_final](../p/pg_checksum_final.md)
  - memcmp
  - [progress_report](../p/progress_report.md)
  - [report_backup_error](../r/report_backup_error.md)
- Called from (representative examples):
  - [verify_backup_checksums](verify_backup_checksums.md) (in pg_verifybackup.c:835)

## Notes and Other Information
- This is a static function within pg_verifybackup.c used internally for individual file checksum verification
- Uses READ_CHUNK_SIZE for efficient chunk-based file reading to minimize memory usage for large files
- Supports multiple checksum algorithms through the pg_checksum API
- Includes robust error handling with descriptive error messages for each potential failure point
- Updates global done_size variable and calls progress_report() to track verification progress
- Performs defensive programming by double-checking file size consistency even though it should be caught earlier
- The function handles both I/O errors and checksum computation errors gracefully
- Memory management is handled by the caller (buffer allocation/deallocation)
- File descriptors are properly closed in all code paths, including error conditions

## Simplified Source

```c
static void
verify_file_checksum(verifier_context *context, manifest_file *m,
                     char *fullpath, uint8 *buffer)
{
    pg_checksum_context checksum_ctx;
    const char *relpath = m->pathname;
    int fd;
    int rc;
    size_t bytes_read = 0;
    uint8 checksumbuf[PG_CHECKSUM_MAX_LENGTH];
    int checksumlen;

    // Open file for reading
    if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) < 0) {
        report_backup_error(context, "could not open file \"%s\": %m", relpath);
        return;
    }

    // Initialize checksum computation
    if (pg_checksum_init(&checksum_ctx, m->checksum_type) < 0) {
        report_backup_error(context, "could not initialize checksum of file \"%s\"", relpath);
        close(fd);
        return;
    }

    // Read file in chunks and update checksum
    while ((rc = read(fd, buffer, READ_CHUNK_SIZE)) > 0) {
        bytes_read += rc;
        if (pg_checksum_update(&checksum_ctx, buffer, rc) < 0) {
            report_backup_error(context, "could not update checksum of file \"%s\"", relpath);
            close(fd);
            return;
        }

        // Update progress
        done_size += rc;
        progress_report(false);
    }

    if (rc < 0)
        report_backup_error(context, "could not read file \"%s\": %m", relpath);

    // Close file
    if (close(fd) != 0) {
        report_backup_error(context, "could not close file \"%s\": %m", relpath);
        return;
    }

    // Check for read errors
    if (rc < 0)
        return;

    // Verify file size matches manifest
    if (bytes_read != m->size) {
        report_backup_error(context,
                           "file \"%s\" should contain %zu bytes, but read %zu bytes",
                           relpath, m->size, bytes_read);
        return;
    }

    // Finalize checksum computation
    checksumlen = pg_checksum_final(&checksum_ctx, checksumbuf);
    if (checksumlen < 0) {
        report_backup_error(context, "could not finalize checksum of file \"%s\"", relpath);
        return;
    }

    // Compare checksum with manifest
    if (checksumlen != m->checksum_length)
        report_backup_error(context,
                           "file \"%s\" has checksum of length %d, but expected %d",
                           relpath, m->checksum_length, checksumlen);
    else if (memcmp(checksumbuf, m->checksum_payload, checksumlen) != 0)
        report_backup_error(context, "checksum mismatch for file \"%s\"", relpath);
}
```