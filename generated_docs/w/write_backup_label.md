# write_backup_label

## Location
[src/bin/pg_combinebackup/backup_label.c:127-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/backup_label.c#L127-L200)

## Overview
Creates a new backup label file in the output directory based on an input backup label, filtering out incremental backup-specific lines and computing checksums.

## Definition


## Detailed Description
The  function generates a new backup_label file by copying the contents of an input backup label buffer while excluding incremental backup-specific information. It creates a file named "backup_label" in the specified output directory, omitting lines that start with "INCREMENTAL FROM LSN:" and "INCREMENTAL FROM TLI:".

The function performs several key operations:
1. Creates and opens the output backup_label file with exclusive creation flags
2. Iterates through the input buffer line by line
3. Filters out incremental backup-specific lines 
4. Writes remaining content to the output file
5. Computes checksums during the write process
6. Optionally adds the file to a backup manifest with metadata

This is essential for creating clean backup label files when combining incremental backups, ensuring the final backup label represents the combined backup state without referencing previous incremental stages.

## Parameters / Member Variables
- : Directory path where the new backup_label file will be created
- : StringInfo buffer containing the source backup label content to process
- : Type of checksum algorithm to use for file integrity verification
- : Optional manifest writer for adding the file to backup manifests (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - pg_checksum_init
  - [get_eol_offset](../g/get_eol_offset.md)
  - [line_starts_with](../l/line_starts_with.md)
  - open
  - write
  - pg_checksum_update
  - close
  - pg_checksum_final
  - add_file_to_manifest
  - [stat](../s/stat.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_combinebackup.c)

## Notes and Other Information
- Uses O_EXCL flag to ensure the backup_label file doesn't already exist, preventing accidental overwrites
- Maintains checksums throughout the write process for data integrity verification
- File permissions are set using pg_file_create_mode for consistent PostgreSQL file permissions
- If manifest writer is provided, the function automatically adds the new file to the backup manifest with size, modification time, and checksum information
- Part of the pg_combinebackup utility infrastructure for merging incremental backups into full backups