# verify_backup_directory

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 610 - 674

## Overview
Recursively verifies a directory within a PostgreSQL backup by scanning its contents and delegating file/subdirectory verification to appropriate functions.

## Definition
```c
static void verify_backup_directory(verifier_context *context, char *relpath,
                                    char *fullpath)
```

## Detailed Description
This function performs recursive directory verification as part of the backup verification process. It opens the specified directory and iterates through all entries, skipping the current and parent directory entries ("." and ".."). For each entry found, it constructs both relative and full paths, checks if the path should be ignored based on the context's ignore list, and then calls verify_backup_file to handle the verification of individual files or subdirectories. The function implements robust error handling, treating failures to open the top-level backup directory as fatal errors while handling subdirectory access failures as non-fatal errors that get added to an ignore list to prevent cascading error reports.

## Parameters / Member Variables
- `context`: Verifier context containing configuration, ignore lists, and error tracking information
- `relpath`: Relative path from the backup root directory (NULL for top-level directory)
- `fullpath`: Complete filesystem path to the directory being verified

## Dependencies
- Functions called/Symbols referenced:
  - opendir
  - readdir
  - closedir
  - report_fatal_error
  - report_backup_error
  - simple_string_list_append
  - should_ignore_relpath
  - verify_backup_file
  - psprintf
  - pstrdup
  - pfree
- Types referenced:
  - verifier_context
  - DIR
  - struct dirent
- Called from (representative examples):
  - main
  - verify_backup_file

## Notes and Other Information
- The function implements recursive directory traversal by calling verify_backup_file, which in turn may call back to this function for subdirectories
- Error handling is context-sensitive: top-level directory failures are fatal, while subdirectory failures are logged and ignored
- The ignore list mechanism prevents repeated error reporting for inaccessible directory trees
- Memory management is handled properly with pfree calls for dynamically allocated path strings
- The function uses PostgreSQL's string handling functions (psprintf, pstrdup) for path manipulation
- Part of the pg_verifybackup utility's core verification pipeline