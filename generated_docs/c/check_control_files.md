# check_control_files

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 594 - 670

## Overview
Performs sanity checks on PostgreSQL control files across multiple backup directories and returns the consistent system identifier found in all backups.

## Definition
```c
static uint64 check_control_files(int n_backups, char **backup_dirs)
```

## Detailed Description
The check_control_files function validates the pg_control files from multiple backup directories to ensure they form a consistent backup set. It performs comprehensive validation including CRC verification, version checking, system identifier consistency, and checksum configuration analysis.

Key functionality includes:
1. **File reading and validation**: Reads each pg_control file and verifies CRC integrity
2. **Version compatibility**: Ensures all control files are from the expected PostgreSQL version
3. **System identifier consistency**: Verifies all backups originate from the same PostgreSQL cluster
4. **Checksum configuration analysis**: Detects mismatches in data checksum settings across backups
5. **Diagnostic output**: Provides debug information and warnings about potential issues

The function processes control files in reverse order (latest backup first) and maintains strict consistency requirements. It warns users about checksum configuration mismatches that could lead to operational issues in the combined backup.

## Parameters / Member Variables
- `n_backups`: Number of backup directories to process
- `backup_dirs`: Array of backup directory paths containing pg_control files

## Dependencies
- Functions called/Symbols referenced:
  - ControlFileData (PostgreSQL control file structure)
  - pg_log_debug (debug logging)
  - get_controlfile_by_exact_path (control file reading utility)
  - PG_CONTROL_VERSION (expected control file version constant)
  - pg_log_warning (warning message logging)
  - pg_log_warning_hint (warning hint message logging)
- Called from (representative examples):
  - main (backup validation workflow)

## Notes and Other Information
- Located in src/bin/pg_combinebackup/pg_combinebackup.c:594-670
- Returns the system identifier that should be consistent across all backups
- Issues warnings when backup checksum configurations are inconsistent
- Includes helpful hint about disabling/re-enabling checksums to avoid failures
- Processes backups in reverse chronological order for efficient validation
- Performs memory cleanup by freeing allocated control file data and path strings
- Validates both CRC integrity and version compatibility before processing control file contents