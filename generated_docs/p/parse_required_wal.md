# parse_required_wal

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 952 - 982

## Overview
Validates the required WAL (Write-Ahead Log) files for backup restoration by using pg_waldump to parse WAL ranges specified in the backup manifest.

## Definition
```c
static void parse_required_wal(verifier_context *context, char *pg_waldump_path,
                               char *wal_directory)
```

## Detailed Description
This function validates that the WAL files required for restoring from the backup are available and parseable. It iterates through all WAL ranges specified in the backup manifest and uses the pg_waldump utility to attempt parsing each range. For each WAL range, it constructs a pg_waldump command with the appropriate timeline, start LSN, and end LSN parameters.

The function executes pg_waldump in quiet mode for each WAL range to verify that the WAL files can be successfully parsed. If pg_waldump fails for any timeline, it reports an error indicating WAL parsing failure. This validation ensures that the backup can be properly restored and that all necessary WAL files are present and intact.

The pg_waldump command is constructed with specific parameters including the WAL directory path, timeline ID, and LSN range boundaries. The function uses the system() call to execute the external pg_waldump utility.

## Parameters / Member Variables
- `context`: Pointer to verifier_context structure containing the backup manifest and error reporting functionality
- `pg_waldump_path`: Full filesystem path to the pg_waldump executable
- `wal_directory`: Directory path containing the WAL files to be validated

## Dependencies
- Functions called/Symbols referenced:
  - psprintf
  - fflush
  - system
  - report_backup_error
  - LSN_FORMAT_ARGS (macro)
- Called from (representative examples):
  - main (in pg_verifybackup.c:374)

## Notes and Other Information
- This is a static function within pg_verifybackup.c used for WAL validation during backup verification
- Uses the external pg_waldump utility to perform actual WAL parsing and validation
- The function processes all WAL ranges linked in the manifest through the first_wal_range->next chain
- pg_waldump is executed with --quiet flag to minimize output noise during validation
- fflush(NULL) is called before system() to ensure output buffering doesn't interfere with command execution
- WAL ranges include timeline ID (tli), start LSN, and end LSN boundaries for precise validation
- Failure of any WAL range parsing results in an error report but doesn't stop processing of remaining ranges
- The function relies on the pg_waldump utility being available in the specified path
- LSN (Log Sequence Number) formatting is handled by the LSN_FORMAT_ARGS macro for proper display
- This validation is crucial for ensuring backup recoverability and WAL continuity