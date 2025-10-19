# parse_tli

## Location
[src/bin/pg_combinebackup/backup_label.c:269-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/backup_label.c#L269-L283)

## Overview
Parses a Timeline ID (TLI) from a string within specified boundaries, validating the format and ensuring proper termination with a newline character.

## Definition
```c
static bool parse_tli(char *s, char *e, TimeLineID *tli)
```

## Detailed Description
The `parse_tli` function is a utility function in the pg_combinebackup tool that parses a Timeline ID from a backup label file. It takes a string pointer range (`s` to `e`) and attempts to extract an unsigned integer representing a Timeline ID. The function validates that the parsed TLI is properly terminated by a newline character, which is required by the backup label file format. The function temporarily null-terminates the string at position `e` to safely use `sscanf`, then restores the original character.

The function is specifically designed to parse TLI values from backup label files, which contain metadata about PostgreSQL backup operations. Timeline IDs are crucial for PostgreSQL's write-ahead logging (WAL) system and help track the history of database changes.

## Parameters / Member Variables
- `s`: Pointer to the start of the string containing the TLI to parse
- `e`: Pointer to the end boundary where parsing should stop
- `tli`: Output parameter - pointer to TimeLineID where the parsed result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - sscanf (standard C library function)
  - TimeLineID (PostgreSQL type definition)
- Called from (representative examples):
  - [parse_backup_label](parse_backup_label.md) (src/bin/pg_combinebackup/backup_label.c:75)
  - [parse_backup_label](parse_backup_label.md) (src/bin/pg_combinebackup/backup_label.c:94)

## Notes and Other Information
- This is a static function, only accessible within the backup_label.c compilation unit
- The function requires that the TLI value be terminated by a newline character (n) for successful parsing
- Uses a temporary null-termination approach to safely parse the string without modifying the original data permanently
- Part of the pg_combinebackup utility, which is used for combining incremental backups
- Timeline IDs must be non-zero values; zero is considered invalid in the calling context
- The function handles parsing validation at two levels: successful sscanf conversion and proper newline termination

## Simplified Source

```c
static bool
parse_tli(char *s, char *e, TimeLineID *tli)
{
    char save = *e;
    int nchars;
    bool success;

    // Temporarily null-terminate for safe parsing
    *e = '\0';
    success = (sscanf(s, "%u%n", tli, &nchars) == 1);
    *e = save;

    // Validate that TLI is terminated by newline
    if (success && s[nchars] != '\n')
        success = false;

    return success;
}
```