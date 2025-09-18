# pg_create_restore_point

## Location
src/backend/access/transam/xlogfuncs.c: 232 - 272

## Overview
Creates a named restore point in the WAL that can be used as a target for point-in-time recovery operations.

## Definition


## Detailed Description
The  function creates a named restore point in the Write-Ahead Log, which serves as a labeled checkpoint that can be referenced during point-in-time recovery operations. This allows database administrators to create meaningful recovery targets with descriptive names rather than relying solely on timestamps or LSN values.

The function performs several validation steps:
1. Ensures the database is not in recovery mode (cannot be run on standby servers)
2. Verifies that WAL level is sufficient ("replica" or "logical") for creating restore points
3. Validates that the restore point name length does not exceed the maximum filename length
4. Creates the actual restore point using 
5. Returns the LSN where the restore point was logged

This functionality is particularly valuable for backup and recovery strategies, allowing administrators to mark specific points in time with meaningful names like "before_major_upgrade" or "end_of_month_processing".

## Parameters / Member Variables
-  (text): User-supplied descriptive name for the restore point (maximum length limited by MAXFNAMELEN - 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - RecoveryInProgress
  - XLogIsNeeded
  - text_to_cstring
  - MAXFNAMELEN
  - XLogRestorePoint
  - PG_RETURN_LSN
- Called from (representative examples):
  - No direct callers found (SQL function interface)

## Notes and Other Information
- This is a PostgreSQL SQL function accessible through the GRANT system for permission management
- Cannot be executed during database recovery - will raise an error if attempted on standby servers
- Requires wal_level to be set to "replica" or "logical" at server startup; insufficient WAL level will cause an error
- Restore point names are limited to MAXFNAMELEN - 1 characters to ensure they can be used as filenames
- The function returns the LSN where the restore point record was logged
- Created restore points can be used with recovery_target_name in postgresql.conf for point-in-time recovery
- Part of PostgreSQL's backup and recovery infrastructure located in src/backend/access/transam/xlogfuncs.c:232-272
- Extremely useful for creating named recovery targets in complex backup and disaster recovery strategies