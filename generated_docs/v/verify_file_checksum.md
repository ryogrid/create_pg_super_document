# verify_file_checksum

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 851 - 951

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
  - pg_checksum_init
  - read
  - pg_checksum_update
  - close
  - pg_checksum_final
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