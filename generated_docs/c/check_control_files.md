# check_control_files

## Location
[src/bin/pg_combinebackup/pg_combinebackup.c:594-670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/pg_combinebackup.c#L594-L670)

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
  - [ControlFileData](../C/ControlFileData.md) (PostgreSQL control file structure)
  - pg_log_debug (debug logging)
  - [get_controlfile_by_exact_path](../g/get_controlfile_by_exact_path.md) (control file reading utility)
  - PG_CONTROL_VERSION (expected control file version constant)
  - pg_log_warning (warning message logging)
  - pg_log_warning_hint (warning hint message logging)
- Called from (representative examples):
  - [main](../m/main.md) (backup validation workflow)

## Notes and Other Information
- Located in src/bin/pg_combinebackup/pg_combinebackup.c:594-670
- Returns the system identifier that should be consistent across all backups
- Issues warnings when backup checksum configurations are inconsistent
- Includes helpful hint about disabling/re-enabling checksums to avoid failures
- Processes backups in reverse chronological order for efficient validation
- Performs memory cleanup by freeing allocated control file data and path strings
- Validates both CRC integrity and version compatibility before processing control file contents

## Simplified Source

```c
static uint64 check_control_files(int n_backups, char **backup_dirs) {
    uint64 system_identifier = 0;
    uint32 data_checksum_version = 0;
    bool data_checksum_mismatch = false;

    // Process each backup's control file (newest to oldest)
    for (int i = n_backups - 1; i >= 0; --i) {
        // Read and validate control file
        char *controlpath = psprintf("%s/global/pg_control", backup_dirs[i]);
        ControlFileData *control_file = get_controlfile_by_exact_path(controlpath, &crc_ok);

        // Verify file integrity and version
        if (!crc_ok)
            pg_fatal("%s: CRC is incorrect", controlpath);
        if (control_file->pg_control_version != PG_CONTROL_VERSION)
            pg_fatal("%s: unexpected control file version", controlpath);

        // Ensure system identifiers match across all backups
        if (i == n_backups - 1)
            system_identifier = control_file->system_identifier;
        else if (system_identifier != control_file->system_identifier)
            pg_fatal("%s: system identifier mismatch", controlpath);

        // Track checksum configuration consistency
        if (i == n_backups - 1)
            data_checksum_version = control_file->data_checksum_version;
        else if (data_checksum_version != 0 &&
                 data_checksum_version != control_file->data_checksum_version)
            data_checksum_mismatch = true;

        // Cleanup memory
        pfree(control_file);
        pfree(controlpath);
    }

    // Warn about checksum configuration mismatches
    if (data_checksum_mismatch) {
        pg_log_warning("only some backups have checksums enabled");
        pg_log_warning_hint("Disable, and optionally reenable, checksums on the output directory to avoid failures.");
    }

    return system_identifier;
}
```