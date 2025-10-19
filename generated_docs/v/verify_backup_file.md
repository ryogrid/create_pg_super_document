# verify_backup_file

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:675-757](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L675-L757)

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
  - [stat](../s/stat.md)
  - [report_backup_error](../r/report_backup_error.md)
  - [simple_string_list_append](../s/simple_string_list_append.md)  
  - [verify_backup_directory](verify_backup_directory.md)
  - manifest_files_lookup
  - [verify_control_file](verify_control_file.md)
  - should_verify_checksum
- Macros/Constants referenced:
  - S_ISDIR
  - S_ISREG
- Types referenced:
  - [verifier_context](verifier_context.md)
  - struct stat
  - [manifest_file](../m/manifest_file.md)
- Called from (representative examples):
  - [verify_backup_directory](verify_backup_directory.md)

## Notes and Other Information
- The function implements a two-phase verification approach: structural validation first, checksum validation later
- Special handling exists for the pg_control file which undergoes additional system identifier verification
- Error handling includes adding problematic paths to an ignore list to prevent cascading error reports
- The matched flag in manifest entries tracks which files have been found on disk
- Progress reporting is updated for files that will undergo checksum verification
- Only regular files and directories are considered valid; other file types trigger errors
- Part of the recursive verification algorithm that processes the entire backup directory tree

## Simplified Source

```c
static void
verify_backup_file(verifier_context *context, char *relpath, char *fullpath)
{
    struct stat sb;
    manifest_file *m;

    // Get file stats
    if (stat(fullpath, &sb) != 0) {
        report_backup_error(context, "could not stat file or directory \"%s\": %m", relpath);
        simple_string_list_append(&context->ignore_list, relpath);
        return;
    }

    // Handle directories recursively
    if (S_ISDIR(sb.st_mode)) {
        verify_backup_directory(context, relpath, fullpath);
        return;
    }

    // Only process regular files
    if (!S_ISREG(sb.st_mode)) {
        report_backup_error(context, "\"%s\" is not a file or directory", relpath);
        return;
    }

    // Look up file in manifest
    m = manifest_files_lookup(context->manifest->files, relpath);
    if (m == NULL) {
        report_backup_error(context, "\"%s\" is present on disk but not in the manifest", relpath);
        return;
    }

    // Mark as found and validate size
    m->matched = true;
    if (m->size != sb.st_size) {
        report_backup_error(context,
                           "\"%s\" has size %lld on disk but size %zu in the manifest",
                           relpath, (long long int) sb.st_size, m->size);
        m->bad = true;
    }

    // Special handling for pg_control file
    if (context->manifest->version != 1 && strcmp(relpath, "global/pg_control") == 0)
        verify_control_file(fullpath, context->manifest->system_identifier);

    // Update progress tracking for checksum verification
    if (show_progress && !skip_checksums && should_verify_checksum(m))
        total_size += m->size;
}
```