# process_directory_recursively

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 823 - 1153

## Overview
Recursively processes directory structures to reconstruct full files from incremental backup files and copies regular files, handling the complete directory tree traversal for backup combination operations.

## Definition


## Detailed Description
This is the core function of pg_combinebackup that handles the recursive processing of PostgreSQL backup directory structures. It performs several critical operations:

1. **Directory Classification**: Identifies special PostgreSQL directories (pg_tblspc, pg_wal, base, global) that require different handling
2. **File Type Processing**: Handles different file types including incremental files (with INCREMENTAL. prefix), regular files, directories, and symbolic links
3. **Incremental File Reconstruction**: Delegates incremental files to reconstruction logic that combines them with prior backup data
4. **Regular File Copying**: Copies non-incremental files directly to the output directory
5. **Manifest Integration**: Reuses checksums from backup manifests when available and generates new manifest entries
6. **Tablespace Handling**: Properly processes both regular tablespaces and in-place tablespaces

The function intelligently skips certain files (backup_label, backup_manifest) and directories (tablespace OID directories in pg_tblspc) that are handled elsewhere in the backup combination process.

## Parameters / Member Variables
- : OID of the tablespace being processed (InvalidOid for main data directory)
- : Source directory path for the current backup
- : Destination directory path for combined output
- : Current subdirectory being processed relative to input/output directories
- : Number of previous backup directories available for reconstruction
- : Array of paths to previous backup directories
- : Array of backup manifest data structures
- : Manifest writer for generating output manifest entries
- : Options structure containing operation settings (dry_run, copy_method, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - : Determines file type from directory entry
  - : Parses OID from directory names
  - : Reconstructs files from incremental data
  - : Copies regular files between directories
  - : Adds entries to backup manifest
  - , : Checksum calculation functions
  - : Looks up file entries in backup manifests
  - , , : Directory traversal functions
  - , : Filesystem operations
- Called from (representative examples):
  -  (in src/bin/pg_combinebackup/pg_combinebackup.c:358)
  -  (in src/bin/pg_combinebackup/pg_combinebackup.c:405)
  -  (recursive self-call at line 983)

## Notes and Other Information
- The function is highly specialized for PostgreSQL backup structures and understands the layout of data directories, tablespaces, and WAL directories
- Incremental files are identified by the INCREMENTAL. prefix and require special reconstruction logic that combines data from multiple backup layers
- WAL directory files are processed without checksums since they're not included in backup manifests
- The function handles both dry-run mode (logging what would be done) and actual execution
- Checksum reuse optimization: when available, checksums from backup manifests are reused instead of recalculating them
- Special handling for pg_tblspc directory prevents recursion into tablespace directories that are processed separately
- The function maintains proper error handling with fatal errors for critical failures and warnings for non-critical issues
- Self-recursive design allows it to handle arbitrarily deep directory structures while maintaining context about the current processing state