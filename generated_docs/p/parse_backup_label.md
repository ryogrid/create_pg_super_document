# parse_backup_label

## Location
src/bin/pg_combinebackup/backup_label.c: 45 - 126

## Overview
Parses a PostgreSQL backup label file to extract essential information required for backup recovery, including start LSN, start timeline ID, and optional incremental backup information.

## Definition


## Detailed Description
The  function processes the contents of a backup label file stored in a StringInfo buffer, starting from the current cursor position. It searches for specific required and optional lines:

- **Required lines**: START WAL LOCATION and START TIMELINE containing the LSN and timeline ID where the backup started
- **Optional lines**: INCREMENTAL FROM LSN and INCREMENTAL FROM TLI for incremental backups (both must be present together or neither)

The function uses a bitmask approach to track which required components have been found and ensures all mandatory elements are present. If incremental backup information is provided, both LSN and TLI must be specified together. The function terminates with fatal errors if required information is missing or malformed.

## Parameters / Member Variables
- : Name of the backup label file being parsed (used for error reporting)
- : StringInfo buffer containing the backup label file contents to parse
- : Output parameter to store the timeline ID where the backup started
- : Output parameter to store the LSN where the backup started
- : Output parameter to store the timeline ID of the previous backup (for incremental backups)
- : Output parameter to store the LSN of the previous backup (for incremental backups)

## Dependencies
- Functions called/Symbols referenced:
  - [get_eol_offset](../g/get_eol_offset.md)
  - [line_starts_with](../l/line_starts_with.md)
  - [parse_lsn](parse_lsn.md)
  - [parse_tli](parse_tli.md)
  - [pg_fatal](pg_fatal.md)
- Called from (representative examples):
  - [check_backup_label_files](../c/check_backup_label_files.md)

## Notes and Other Information
- The function initializes output parameters to safe default values (0 for timeline IDs, InvalidXLogRecPtr for LSNs)
- Uses a bitmask to track found components: bit 1 for START WAL LOCATION, bit 2 for START TIMELINE, bit 4 for INCREMENTAL FROM LSN, bit 8 for INCREMENTAL FROM TLI
- Enforces that incremental backup information must be complete (both LSN and TLI) or absent
- Part of the pg_combinebackup utility for combining incremental backups
- All parsing errors result in fatal termination with descriptive error messages