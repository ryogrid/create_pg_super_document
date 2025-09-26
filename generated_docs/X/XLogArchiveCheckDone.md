# XLogArchiveCheckDone

## Location
[src/backend/access/transam/xlogarchive.c:565-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogarchive.c#L565-L618)

## Overview
Determines whether an old XLOG segment file can be safely deleted by checking its archive status and ensuring proper archival notification exists.

## Definition
bool XLogArchiveCheckDone(const char *xlog)

## Detailed Description
XLogArchiveCheckDone checks if a WAL segment file is ready for deletion by examining its archival status. The function implements a comprehensive logic that considers the current archive mode and recovery state:

- Returns true immediately if archive_mode is "off" (no archiving required)
- Returns true during archive recovery when archive_mode is not "always"  
- For active archiving scenarios, checks for .done file (archival complete) and returns true if found
- If .ready file exists, returns false (archival in progress)
- Includes race condition handling by double-checking for .done files
- Creates a .ready file via XLogArchiveNotify if none exists, ensuring the archiver will process the file

This function ensures WAL files are not prematurely deleted before successful archival.

## Parameters / Member Variables
- : The name of the XLOG segment file to check for deletion readiness

## Dependencies
- Functions called/Symbols referenced:
  - XLogArchivingActive
  - XLogArchivingAlways
  - [GetRecoveryState](../G/GetRecoveryState.md)
  - [StatusFilePath](../S/StatusFilePath.md)
  - [XLogArchiveNotify](XLogArchiveNotify.md)
  - RECOVERY_STATE_ARCHIVE
- Called from (representative examples):
  - [RemoveOldXlogFiles](../R/RemoveOldXlogFiles.md)
  - [CleanupBackupHistory](../C/CleanupBackupHistory.md)

## Notes and Other Information
- Implements retry logic for .ready file creation to handle transient failures
- Handles race conditions by double-checking .done status before creating .ready files
- [Archive](../A/Archive.md) mode settings (off/on/always) directly influence deletion eligibility
- Critical for preventing data loss by ensuring WAL files are archived before deletion
- The function's logic adapts behavior based on whether the server is a primary or standby