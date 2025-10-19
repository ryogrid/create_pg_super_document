# parse_backup_label

## Location
[src/bin/pg_combinebackup/backup_label.c:45-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/backup_label.c#L45-L126)

## Overview
Parses a PostgreSQL backup label file to extract essential information required for backup recovery, including start LSN, start timeline ID, and optional incremental backup information.

## Definition

```c
void
parse_backup_label(char *filename, StringInfo buf,
				   TimeLineID *start_tli, XLogRecPtr *start_lsn,
				   TimeLineID *previous_tli, XLogRecPtr *previous_lsn)
```
## Detailed Description
The  function processes the contents of a backup label file stored in a StringInfo buffer, starting from the current cursor position. It searches for specific required and optional lines:

- **Required lines**: START WAL LOCATION and START TIMELINE containing the LSN and timeline ID where the backup started
- **Optional lines**: INCREMENTAL FROM LSN and INCREMENTAL FROM TLI for incremental backups (both must be present together or neither)

The function uses a bitmask approach to track which required components have been found and ensures all mandatory elements are present. If incremental backup information is provided, both LSN and TLI must be specified together. The function terminates with fatal errors if required information is missing or malformed.

## Parameters / Member Variables
- `*filename`: Name of the backup label file being parsed (used for error reporting)
- `buf`: StringInfo buffer containing the backup label file contents to parse
- `*start_tli`: Output parameter to store the timeline ID where the backup started
- `*start_lsn`: Output parameter to store the LSN where the backup started
- `*previous_tli`: Output parameter to store the timeline ID of the previous backup (for incremental backups)
- `*previous_lsn`: Output parameter to store the LSN of the previous backup (for incremental backups)
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

## Simplified Source

```c
void
parse_backup_label(char *filename, StringInfo buf,
                   TimeLineID *start_tli, XLogRecPtr *start_lsn,
                   TimeLineID *previous_tli, XLogRecPtr *previous_lsn)
{
    int found = 0;

    // Initialize output parameters
    *start_tli = 0;
    *start_lsn = InvalidXLogRecPtr;
    *previous_tli = 0;
    *previous_lsn = InvalidXLogRecPtr;

    // Parse each line in the buffer
    while (buf->cursor < buf->len) {
        char *s = &buf->data[buf->cursor];
        int eo = get_eol_offset(buf);
        char *e = &buf->data[eo];
        char *c;

        // Parse START WAL LOCATION line
        if (line_starts_with(s, e, "START WAL LOCATION: ", &s)) {
            if (!parse_lsn(s, e, start_lsn, &c) || c >= e || *c != ' ')
                pg_fatal("%s: could not parse START WAL LOCATION", filename);
            found |= 1;
        }
        // Parse START TIMELINE line
        else if (line_starts_with(s, e, "START TIMELINE: ", &s)) {
            if (!parse_tli(s, e, start_tli) || *start_tli == 0)
                pg_fatal("%s: could not parse START TIMELINE", filename);
            found |= 2;
        }
        // Parse INCREMENTAL FROM LSN line
        else if (line_starts_with(s, e, "INCREMENTAL FROM LSN: ", &s)) {
            if (!parse_lsn(s, e, previous_lsn, &c) || c >= e || *c != '\n')
                pg_fatal("%s: could not parse INCREMENTAL FROM LSN", filename);
            found |= 4;
        }
        // Parse INCREMENTAL FROM TLI line
        else if (line_starts_with(s, e, "INCREMENTAL FROM TLI: ", &s)) {
            if (!parse_tli(s, e, previous_tli) || *previous_tli == 0)
                pg_fatal("%s: could not parse INCREMENTAL FROM TLI", filename);
            found |= 8;
        }

        buf->cursor = eo;
    }

    // Validate required fields are present
    if ((found & 1) == 0)
        pg_fatal("%s: could not find START WAL LOCATION", filename);
    if ((found & 2) == 0)
        pg_fatal("%s: could not find START TIMELINE", filename);

    // Validate incremental backup fields consistency
    if ((found & 4) != 0 && (found & 8) == 0)
        pg_fatal("%s: INCREMENTAL FROM LSN requires INCREMENTAL FROM TLI", filename);
    if ((found & 8) != 0 && (found & 4) == 0)
        pg_fatal("%s: INCREMENTAL FROM TLI requires INCREMENTAL FROM LSN", filename);
}
```