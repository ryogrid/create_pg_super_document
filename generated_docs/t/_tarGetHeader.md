# _tarGetHeader

## Location
[src/bin/pg_dump/pg_backup_tar.c:1140-1211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L1140-L1211)

## Overview
Reads and verifies a TAR header block from a tar archive file during PostgreSQL pg_dump restore operations.

## Definition
```c
static int _tarGetHeader(ArchiveHandle *AH, TAR_MEMBER *th)
```

## Detailed Description
This function is a core component of PostgreSQL's TAR archive handling in pg_dump/pg_restore operations. It reads a TAR_BLOCK_SIZE (512 bytes) header block from the archive file, validates its integrity using checksum verification, and extracts essential file information including filename and size.

The function implements robust error handling by:
- Reading raw TAR blocks and handling EOF conditions gracefully
- Verifying TAR header checksums to detect corruption
- Skipping null blocks (empty padding blocks in TAR format)
- Extracting and null-terminating filenames from the TAR header
- Providing detailed logging and error reporting

The function follows the POSIX TAR format specification, parsing standard TAR header fields at specific byte offsets within the 512-byte header block.

## Parameters / Member Variables
- `AH`: Archive handle containing the format-specific data and file handles for the TAR archive
- `th`: TAR_MEMBER structure that will be populated with the extracted header information (filename and file length)

## Dependencies
- Functions called/Symbols referenced:
  - [_tarReadRaw](_tarReadRaw.md)
  - [tarChecksum](tarChecksum.md)
  - [read_tar_number](../r/read_tar_number.md)
  - [strlcpy](../s/strlcpy.md)
  - pg_log_debug
  - [pg_fatal](../p/pg_fatal.md)
  - ngettext
  - ftello
- Called from (representative examples):
  - [_tarPositionTo](_tarPositionTo.md)

## Notes and Other Information
- Returns 1 on successful header read, 0 on EOF
- The function handles TAR format specifics like 100-byte filename fields that may not be null-terminated
- Implements checksum validation according to TAR format standards
- Uses pgoff_t for file position and size handling to support large files
- Provides multilingual error messages using ngettext for proper pluralization
- File position tracking is maintained for debugging and error reporting purposes

## Simplified Source

```c
static int _tarGetHeader(ArchiveHandle *AH, TAR_MEMBER *th) {
    lclContext *ctx = (lclContext *) AH->formatData;
    char header_block[TAR_BLOCK_SIZE];
    char filename[101];
    int calculated_checksum, stored_checksum;
    pgoff_t file_length;
    bool found_valid_block = false;

    while (!found_valid_block) {
        // Read next TAR block (512 bytes)
        pgoff_t bytes_read = _tarReadRaw(AH, header_block, TAR_BLOCK_SIZE, NULL, ctx->tarFH);

        if (bytes_read == 0)  // EOF
            return 0;

        if (bytes_read != TAR_BLOCK_SIZE)
            pg_fatal("incomplete tar header found (%lu bytes)", (unsigned long) bytes_read);

        // Verify checksum
        calculated_checksum = tarChecksum(header_block);
        stored_checksum = read_tar_number(&header_block[TAR_OFFSET_CHECKSUM], 8);

        if (calculated_checksum == stored_checksum) {
            found_valid_block = true;
        } else {
            // Check if this is a null block (padding) - skip if so
            bool is_null_block = true;
            for (int i = 0; i < TAR_BLOCK_SIZE; i++) {
                if (header_block[i] != 0) {
                    is_null_block = false;
                    break;
                }
            }
            if (!is_null_block)
                found_valid_block = true;  // Corrupted but non-null block
        }
    }

    // Extract filename (may not be null-terminated in TAR format)
    strlcpy(filename, &header_block[TAR_OFFSET_NAME], 101);

    // Extract file size
    file_length = read_tar_number(&header_block[TAR_OFFSET_SIZE], 12);

    // Final checksum validation for non-null blocks
    if (calculated_checksum != stored_checksum)
        pg_fatal("corrupt tar header found in %s (expected %d, computed %d)",
                 filename, stored_checksum, calculated_checksum);

    // Populate TAR_MEMBER structure
    th->targetFile = pg_strdup(filename);
    th->fileLen = file_length;

    return 1;  // Success
}
```