# verify_backup_checksums

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 811 - 850

## Overview
Verifies the checksums of all files in the backup manifest that are eligible for checksum verification and have not already encountered problems.

## Definition
```c
static void verify_backup_checksums(verifier_context *context)
```

## Detailed Description
This function performs checksum verification on backup files to ensure data integrity. It iterates through all files in the backup manifest and selectively verifies checksums for files that meet the verification criteria. The function skips files that already have reported problems, lack checksums, or should be ignored based on the verification context.

The verification process involves reading each qualifying file from disk and computing its checksum, then comparing it against the expected checksum stored in the manifest. A buffer is allocated for efficient file reading during the checksum computation process. Progress reporting is enabled during this operation to provide feedback on the verification progress.

## Parameters / Member Variables
- `context`: Pointer to verifier_context structure containing backup directory path, manifest data, and verification state information

## Dependencies
- Functions called/Symbols referenced:
  - progress_report
  - pg_malloc
  - manifest_files_start_iterate
  - manifest_files_iterate
  - should_verify_checksum
  - should_ignore_relpath
  - psprintf
  - verify_file_checksum
  - pfree
- Called from (representative examples):
  - main (in pg_verifybackup.c:367)

## Notes and Other Information
- This is a static function within pg_verifybackup.c used internally for backup verification
- Uses READ_CHUNK_SIZE constant for buffer allocation to optimize file reading performance
- Progress reporting is enabled both at the start and end of the verification process
- Memory management is carefully handled with proper allocation and deallocation of buffers and path strings
- The function constructs full file paths by combining the backup directory with relative paths from the manifest
- Checksum verification is conditional based on should_verify_checksum() and should_ignore_relpath() filters
- This function represents the core checksum verification phase of the backup validation process