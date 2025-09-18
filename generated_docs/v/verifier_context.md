# verifier_context

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 111 - 118

## Overview
A context structure that maintains all state and configuration information needed during backup verification operations, serving as the central coordination point for the pg_verifybackup tool.

## Definition
```c
typedef struct verifier_context
{
    manifest_data       *manifest;
    char               *backup_directory;
    SimpleStringList    ignore_list;
    bool                exit_on_error;
    bool                saw_any_error;
} verifier_context;
```

## Detailed Description
The `verifier_context` structure encapsulates all the state and configuration needed to perform backup verification operations. It holds references to the parsed manifest data, the backup directory being verified, a list of files/patterns to ignore during verification, and flags to control error handling behavior. This structure is passed through the verification workflow to maintain consistency and provide centralized access to verification parameters and state.

## Parameters / Member Variables
- `manifest`: Pointer to the parsed manifest data containing all backup metadata and file information
- `backup_directory`: Path to the backup directory being verified
- `ignore_list`: List of file patterns or paths that should be ignored during verification
- `exit_on_error`: Boolean flag controlling whether verification should stop immediately upon encountering an error
- `saw_any_error`: Boolean flag tracking whether any errors have been encountered during the verification process

## Dependencies
- Functions called/Symbols referenced:
  - manifest_data
  - SimpleStringList

- Called from (representative examples):
  - main (src/bin/pg_verifybackup/pg_verifybackup.c:192)
  - verify_backup_directory (src/bin/pg_verifybackup/pg_verifybackup.c:610)
  - verify_backup_file (src/bin/pg_verifybackup/pg_verifybackup.c:675)
  - report_extra_backup_files (src/bin/pg_verifybackup/pg_verifybackup.c:791)
  - verify_backup_checksums (src/bin/pg_verifybackup/pg_verifybackup.c:811)
  - parse_required_wal (src/bin/pg_verifybackup/pg_verifybackup.c:952)
  - report_backup_error (src/bin/pg_verifybackup/pg_verifybackup.c:983)

## Notes and Other Information
This structure is central to the pg_verifybackup tool workflow, providing thread-safe state management and configuration throughout the verification process. The ignore_list feature allows users to exclude certain files from verification, which is useful for temporary or variable files that may not be included in the manifest. The error handling flags provide flexibility in verification behavior, allowing both fail-fast and comprehensive verification modes.