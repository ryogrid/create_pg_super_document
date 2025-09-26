# update_controlfile

## Location
src/common/controldata_utils.c: 189 - 283

## Overview
The update_controlfile function writes updated control file data to disk with proper timestamp updating, CRC calculation, and optional synchronization, handling both backend and frontend environments.

## Definition
void update_controlfile(const char *DataDir, ControlFileData *ControlFile, bool do_sync)

## Detailed Description
This function provides the core mechanism for updating PostgreSQL's control file with new data. It performs several critical operations including updating the modification timestamp, recalculating the CRC checksum, and writing the data to disk with proper error handling and optional synchronization.

Key operations performed:
- Updates the control file's timestamp to the current time
- Recalculates CRC32C checksum for data integrity
- Zero-padds the data to PG_CONTROL_FILE_SIZE to prevent premature EOF issues
- Writes the data using appropriate file operations for backend vs frontend
- Optionally syncs the data to disk for durability
- Provides comprehensive error handling with PANIC in backend, pg_fatal in frontend

The function uses different file handling strategies: BasicOpenFile in the backend (since PANIC errors don't need cleanup) and regular open in frontend environments. It also includes wait event reporting for monitoring in backend environments.

## Parameters / Member Variables
- : The PostgreSQL data directory path where the control file should be written
- : Pointer to the ControlFileData structure containing the updated control file information
- : Boolean flag indicating whether to fsync the file after writing for immediate durability

## Dependencies
- Functions called/Symbols referenced:
  - time (to update timestamp)
  - INIT_CRC32C, COMP_CRC32C, FIN_CRC32C (CRC calculation macros)
  - memset, memcpy (memory operations)
  - snprintf (path construction)
  - BasicOpenFile/open (file opening)
  - write (file writing)
  - pg_fsync/fsync (file synchronization)
  - close (file closing)
  - pgstat_report_wait_start/pgstat_report_wait_end (backend wait events)
  - ereport/pg_fatal (error reporting)
- Called from (representative examples):
  - UpdateControlFile
  - modify_subscriber_sysid
  - RewriteControlFile
  - perform_rewind

## Notes and Other Information
- Caller must properly lock ControlFileLock when calling from backend to prevent concurrent modifications
- Uses PANIC errors in backend since control file corruption is a critical system failure
- Updates timestamp automatically on each write to track last modification time
- Zero-pads output to PG_CONTROL_FILE_SIZE (8192 bytes) to maintain consistent file size
- CRC covers all control file data except the CRC field itself
- Wait event reporting in backend allows monitoring of control file write performance
- The do_sync parameter allows callers to control whether writes are immediately flushed to storage
- Critical for maintaining cluster state consistency across restarts and recovery operations