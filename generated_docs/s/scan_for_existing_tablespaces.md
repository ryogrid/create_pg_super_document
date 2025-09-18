# scan_for_existing_tablespaces

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 1245 - 1375

## Overview
Scans the pg_tblspc directory of the final input backup to create a canonical list of tablespaces that are part of the backup, handling both in-place and relocated tablespaces.

## Definition
```c
static cb_tablespace *scan_for_existing_tablespaces(char *pathname, cb_options *opt)
```

## Detailed Description
This function examines the pg_tblspc directory within a backup directory to identify all tablespaces. It processes both symbolic links (representing external tablespaces) and directories (representing in-place tablespaces). For symbolic links, it reads the link target, validates it, and matches it with provided tablespace mappings. For directories, it treats them as in-place tablespaces. The function creates a linked list of cb_tablespace structures containing tablespace information including OIDs, old/new directory paths, and in-place status. It performs extensive validation including OID parsing, path canonicalization, and ensures no duplicate tablespace destinations.

## Parameters / Member Variables
- `pathname`: Path to the toplevel backup directory for the final backup in the backup chain
- `opt`: cb_options structure containing program options including tablespace mappings

## Dependencies
- Functions called/Symbols referenced:
  - DIR, dirent (directory handling types)
  - cb_tablespace, cb_options, cb_tablespace_mapping (structure types)
  - pg_log_debug (logging function)
  - opendir, readdir, closedir (directory operations)
  - parse_oid (OID parsing utility)
  - get_dirent_type (file type detection)
  - pg_malloc0 (zero-initialized memory allocation)
  - readlink (symbolic link reading)
  - is_absolute_path (path validation)
  - canonicalize_path (path canonicalization)
  - strlcpy (safe string copying)
- Called from (representative examples):
  - main (in src/bin/pg_combinebackup/pg_combinebackup.c:311)

## Notes and Other Information
- This is a static function used specifically within pg_combinebackup utility
- Returns a linked list of cb_tablespace structures representing discovered tablespaces
- Handles both external tablespaces (symbolic links) and in-place tablespaces (directories)
- Requires tablespace mappings for all external tablespaces or will fatal error
- Performs validation to prevent tablespace conflicts and invalid configurations
- Uses errno handling for proper directory iteration error detection
- File location: src/bin/pg_combinebackup/pg_combinebackup.c:1245-1375