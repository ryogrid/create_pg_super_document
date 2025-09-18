# xl_restore_point

## Location
src/include/access/xlog_internal.h: 286 - 290

## Overview
A data structure that logs restore point information in the WAL, allowing users to create named recovery targets for point-in-time recovery operations.

## Definition


## Detailed Description
xl_restore_point is a WAL record structure used to log user-created restore points in the transaction log. Restore points are named markers in the WAL stream that can be used as recovery targets during point-in-time recovery (PITR). When a restore point is created using the pg_create_restore_point() function, this structure is written to the WAL with the XLOG_RESTORE_POINT record type (0x70).

The restore point includes both a timestamp indicating when it was created and a user-defined name that can be referenced during recovery operations. This allows database administrators to create meaningful recovery targets at specific points in time, making it easier to recover to a known good state.

## Parameters / Member Variables
- : Timestamp (with timezone) indicating when the restore point was created
- : User-defined name for the restore point, limited to MAXFNAMELEN (64) characters

## Dependencies
- Functions called/Symbols referenced:
  - MAXFNAMELEN (maximum filename length constant, value 64)
  - TimestampTz (timestamp with timezone type)
- Called from (representative examples):
  - [XLogRestorePoint](../X/XLogRestorePoint.md) (creates and logs restore point records)
  - [xlog_desc](xlog_desc.md) (describes restore point records for debugging)
  - [getRecordTimestamp](../g/getRecordTimestamp.md) (extracts timestamp from restore point records)
  - [recoveryStopsAfter](../r/recoveryStopsAfter.md) (checks if recovery should stop at restore point)

## Notes and Other Information
- Associated with WAL record type XLOG_RESTORE_POINT (0x70)
- Created by pg_create_restore_point() SQL function
- Used for point-in-time recovery (PITR) operations
- The name length is limited by MAXFNAMELEN (64 characters)
- Restore points can be used as recovery_target_name in recovery.conf
- Provides a user-friendly way to specify recovery targets instead of using LSN or timestamp
- Part of PostgreSQL's backup and recovery infrastructure