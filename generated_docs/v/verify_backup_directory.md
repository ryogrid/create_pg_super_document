# verify_backup_directory

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:610-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L610-L674)

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
  - [opendir](../o/opendir.md)
  - [readdir](../r/readdir.md)
  - [closedir](../c/closedir.md)
  - [report_fatal_error](../r/report_fatal_error.md)
  - [report_backup_error](../r/report_backup_error.md)
  - [simple_string_list_append](../s/simple_string_list_append.md)
  - [should_ignore_relpath](../s/should_ignore_relpath.md)
  - [verify_backup_file](verify_backup_file.md)
  - [psprintf](../p/psprintf.md)
  - [pstrdup](../p/pstrdup.md)
  - [pfree](../p/pfree.md)
- Types referenced:
  - [verifier_context](verifier_context.md)
  - [DIR](../D/DIR.md)
  - struct dirent
- Called from (representative examples):
  - [main](../m/main.md)
  - [verify_backup_file](verify_backup_file.md)

## Notes and Other Information
- The function implements recursive directory traversal by calling verify_backup_file, which in turn may call back to this function for subdirectories
- Error handling is context-sensitive: top-level directory failures are fatal, while subdirectory failures are logged and ignored
- The ignore list mechanism prevents repeated error reporting for inaccessible directory trees
- Memory management is handled properly with pfree calls for dynamically allocated path strings
- The function uses PostgreSQL's string handling functions (psprintf, pstrdup) for path manipulation
- Part of the pg_verifybackup utility's core verification pipeline

## Simplified Source

```c
static void
verify_backup_directory(verifier_context *context, char *relpath, char *fullpath)
{
    DIR *dir;
    struct dirent *dirent;

    // Open directory
    dir = opendir(fullpath);
    if (dir == NULL) {
        // Top-level directory failure is fatal
        if (relpath == NULL)
            report_fatal_error("could not open directory \"%s\": %m", fullpath);

        // Subdirectory failure is non-fatal, add to ignore list
        report_backup_error(context, "could not open directory \"%s\": %m", fullpath);
        simple_string_list_append(&context->ignore_list, relpath);
        return;
    }

    // Process each directory entry
    while (errno = 0, (dirent = readdir(dir)) != NULL) {
        char *filename = dirent->d_name;
        char *newfullpath = psprintf("%s/%s", fullpath, filename);
        char *newrelpath;

        // Skip "." and ".."
        if (filename[0] == '.' && (filename[1] == '\0' || strcmp(filename, "..") == 0))
            continue;

        // Build relative path
        if (relpath == NULL)
            newrelpath = pstrdup(filename);
        else
            newrelpath = psprintf("%s/%s", relpath, filename);

        // Verify file/subdirectory if not ignored
        if (!should_ignore_relpath(context, newrelpath))
            verify_backup_file(context, newrelpath, newfullpath);

        pfree(newfullpath);
        pfree(newrelpath);
    }

    // Close directory
    if (closedir(dir))
        report_backup_error(context, "could not close directory \"%s\": %m", fullpath);
}
```