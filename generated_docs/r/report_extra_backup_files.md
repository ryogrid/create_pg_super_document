# report_extra_backup_files

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:791-810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L791-L810)

## Overview
Reports files that are present in the backup manifest but not found on disk during backup verification.

## Definition

```c
static void
report_extra_backup_files(verifier_context *context)
```
## Detailed Description
This function scans through all files listed in the backup manifest and identifies those that are marked as unmatched (not found on disk). For each unmatched file that should not be ignored based on the verification context, it reports an error indicating that the file exists in the manifest but is missing from the actual backup data on disk. This is a critical verification step that ensures backup integrity by detecting incomplete or corrupted backups.

The function iterates through the manifest files using the manifest iterator interface and checks each file's 'matched' flag. Files with unset 'matched' flags indicate they were expected but not found during the backup verification process.

## Parameters / Member Variables
- `context`: Pointer to verifier_context structure containing the backup manifest and verification state information

## Dependencies
- Functions called/Symbols referenced:
  - manifest_files_start_iterate
  - manifest_files_iterate  
  - [should_ignore_relpath](../s/should_ignore_relpath.md)
  - [report_backup_error](report_backup_error.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_verifybackup.c:360)

## Notes and Other Information
- This is a static function within pg_verifybackup.c, used internally for backup verification
- The function relies on the 'matched' flag being properly set during the file verification process
- Files that should be ignored (as determined by should_ignore_relpath) are not reported as errors
- Each missing file generates an individual error report through report_backup_error
- This function is typically called near the end of the verification process after all files have been checked

## Simplified Source

```c
static void
report_extra_backup_files(verifier_context *context)
{
    manifest_data *manifest = context->manifest;
    manifest_files_iterator it;
    manifest_file *m;

    // Iterate through all manifest files
    manifest_files_start_iterate(manifest->files, &it);
    while ((m = manifest_files_iterate(manifest->files, &it)) != NULL) {
        // Report unmatched files that shouldn't be ignored
        if (!m->matched && !should_ignore_relpath(context, m->pathname))
            report_backup_error(context,
                               "\"%s\" is present in the manifest but not on disk",
                               m->pathname);
    }
}
```