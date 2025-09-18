# relmap_redo

## Location
src/backend/utils/cache/relmapper.c: 1096 - 1141

## Overview
Handles replay of write-ahead log (WAL) records for relation mapping changes during PostgreSQL recovery, reconstructing relation-to-filenode mappings from logged updates.

## Definition


## Detailed Description
The  function is the resource manager routine responsible for replaying WAL records that contain relation mapping updates during database recovery. PostgreSQL maintains mapping files that associate logical relation OIDs with physical filenode numbers, and when these mappings change, they are logged to WAL for crash recovery purposes.

This function processes  records during recovery, extracting the new mapping data and writing it to the appropriate relmap file on disk. The function handles both shared catalog mappings (system-wide) and database-specific mappings, ensuring that the relation-to-filenode associations are correctly restored during recovery operations.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record with relation mapping update information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecHasAnyBlockRefs  
  - XLogRecGetData
  - GetDatabasePath
  - LWLockAcquire
  - write_relmap_file
  - LWLockRelease
  - pfree
  - elog
- Called from (representative examples):
  - MinSizeOfRelmapUpdate (referenced in header)

## Notes and Other Information
- Only processes  record types; panics on unknown operation codes
- Uses exclusive RelationMappingLock to prevent conflicts with concurrent relmap file loads during recovery
- Validates that WAL record data size matches expected RelMapFile structure size
- Does not write new WAL entries during replay (prevents infinite recursion)
- Handles both new database creation and existing database relmap updates with the same record type
- The function acquires locks and sends sinval messages even for new database creation cases, though unnecessary, for code simplicity
- Located in src/backend/utils/cache/relmapper.c:1096-1141