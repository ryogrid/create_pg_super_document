# pg_control_recovery

## Location
src/backend/utils/misc/pg_controldata.c: 163 - 203

## Overview
A PostgreSQL SQL function that retrieves recovery-related information from the control file, providing key details about backup and recovery state.

## Definition


## Detailed Description
The  function extracts and returns recovery-specific information stored in PostgreSQL's control file. This function is essential for monitoring backup and recovery operations, providing visibility into minimum recovery points, backup boundaries, and recovery requirements. It safely reads the control file under lock protection and returns recovery state information that is critical for understanding the database's backup status and recovery capabilities.

## Parameters / Member Variables
- No input parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Returns a composite tuple containing 5 fields:
  - : LSN that must be reached during recovery
  - : Timeline ID for the minimum recovery point
  - : LSN where backup started (if in backup mode)
  - : LSN where backup ended (if completed)
  - : Whether backup end record is required for consistency

## Dependencies
- Functions called/Symbols referenced:
  - get_call_result_type: Validates return type
  - LWLockAcquire/LWLockRelease: Manages concurrent access to control file
  - get_controlfile: Reads and parses the control file
  - LSNGetDatum: Converts LSN values to PostgreSQL Datum format
  - Int32GetDatum: Converts integer values to Datum format  
  - BoolGetDatum: Converts boolean values to Datum format
  - heap_form_tuple: Creates the return tuple
  - ControlFileData: Structure containing control file data
- Called from (representative examples):
  - SQL queries via function call mechanism

## Notes and Other Information
- Requires shared lock on ControlFileLock to ensure consistent reads
- Validates control file CRC checksum and raises ERROR if corrupted
- Critical for backup and recovery monitoring and planning
- Minimum recovery point indicates the furthest point that must be reached during recovery
- Backup start/end points track online backup boundaries
- backup_end_required flag indicates if backup completion record is mandatory
- Part of the administrative interface for recovery state monitoring
- Located in src/backend/utils/misc/pg_controldata.c:163-203