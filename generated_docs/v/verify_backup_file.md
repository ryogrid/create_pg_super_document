# verify_backup_file

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 675 - 757

## Overview
Verifies a single file system entry (file, directory, or symlink) against the backup manifest, handling size validation, manifest lookup, and delegating to appropriate verification functions.

## Definition
```c
static void verify_backup_file(verifier_context *context, char *relpath, char *fullpath)
```

## Detailed Description
This function serves as the main entry point for verifying individual filesystem entries during backup verification. It first attempts to stat the given path to determine its type and properties. For directories, it delegates to verify_backup_directory for recursive processing. For regular files, it performs several validation steps: checks if the file exists in the backup manifest, validates that the file size matches the manifest entry, and marks the manifest entry as matched. The function implements special handling for the pg_control file by calling verify_control_file when appropriate. It also updates progress tracking statistics for checksum verification. Notably, actual checksum verification is deferred to a later phase to prioritize reporting structural issues quickly.

## Parameters / Member Variables
- `context`: Verifier context containing manifest data, configuration, and error tracking
- `relpath`: Relative path from the backup root directory to the file being verified  
- `fullpath`: Complete filesystem path to the file being verified

## Dependencies
- Functions called/Symbols referenced:
  - stat
  - report_backup_error
  - simple_string_list_append  
  - verify_backup_directory
  - manifest_files_lookup
  - verify_control_file
  - should_verify_checksum
- Macros/Constants referenced:
  - S_ISDIR
  - S_ISREG
- Types referenced:
  - verifier_context
  - struct stat
  - manifest_file
- Called from (representative examples):
  - verify_backup_directory

## Notes and Other Information
- The function implements a two-phase verification approach: structural validation first, checksum validation later
- Special handling exists for the pg_control file which undergoes additional system identifier verification
- Error handling includes adding problematic paths to an ignore list to prevent cascading error reports
- The matched flag in manifest entries tracks which files have been found on disk
- Progress reporting is updated for files that will undergo checksum verification
- Only regular files and directories are considered valid; other file types trigger errors
- Part of the recursive verification algorithm that processes the entire backup directory tree