# createBackupLabel

## Location
[src/bin/pg_rewind/pg_rewind.c:961-1003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/pg_rewind.c#L961-L1003)

## Overview
createBackupLabel creates a backup_label file that forces PostgreSQL recovery to begin at the last common checkpoint during a pg_rewind operation.

## Definition


## Detailed Description
This function generates a backup_label file in the target data directory, which is a critical component for PostgreSQL recovery. The backup_label file instructs PostgreSQL to start recovery from a specific WAL location rather than relying on the control file's checkpoint information.

The function performs several key operations:
1. Converts the start point LSN to WAL segment information
2. Generates the corresponding WAL filename using the timeline ID and segment number
3. Creates a timestamp for the backup operation
4. Formats a backup label with all necessary recovery information including start WAL location, checkpoint location, backup method, and timestamp
5. Writes the formatted content to the backup_label file in the target directory

The backup_label format follows PostgreSQL's standard structure but is specifically tailored for pg_rewind operations, identifying the backup method as 'pg_rewind' and backup source as 'standby'.

## Parameters / Member Variables
- : XLogRecPtr indicating the WAL location where recovery should begin
- : TimeLineID specifying the timeline for the recovery start point
- : XLogRecPtr indicating the checkpoint location to be recorded in the backup label

## Dependencies
- Functions called/Symbols referenced:
  - XLByteToSeg (converts LSN to WAL segment number)
  - [XLogFileName](../X/XLogFileName.md) (generates WAL filename from timeline and segment)
  - time (gets current time)
  - localtime (converts time to local time structure)
  - strftime (formats timestamp string)
  - snprintf (formats backup label content)
  - [open_target_file](../o/open_target_file.md) (opens target file for writing)
  - [write_target_range](../w/write_target_range.md) (writes data to target file)
  - [close_target_file](close_target_file.md) (closes target file)
  - [pg_fatal](../p/pg_fatal.md) (fatal error reporting)
- Called from (representative examples):
  - [perform_rewind](../p/perform_rewind.md) (during the main rewind operation)

## Notes and Other Information
- This is a static function local to pg_rewind.c
- The backup_label file is essential for proper PostgreSQL recovery after pg_rewind
- Contains a TODO comment about handling existing backup_label files
- Uses LSN_FORMAT_ARGS macro for consistent LSN formatting in hexadecimal
- The backup method is hardcoded as 'pg_rewind' and backup source as 'standby'
- Buffer overflow protection is implemented with size checking
- Part of pg_rewind's final phase where the target directory is prepared for recovery