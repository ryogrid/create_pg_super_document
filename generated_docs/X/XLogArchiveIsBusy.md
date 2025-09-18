# XLogArchiveIsBusy

## Location
[src/backend/access/transam/xlogarchive.c:619-663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogarchive.c#L619-L663)

## Overview
Checks whether an XLOG segment file is still unarchived by examining its status files and physical existence.

## Definition
bool XLogArchiveIsBusy(const char *xlog)

## Detailed Description
XLogArchiveIsBusy determines if a WAL segment file is still waiting for or undergoing archival. The function implements a sophisticated checking mechanism:

- First checks for .done file - if present, returns false (not busy, archival complete)
- Then checks for .ready file - if present, returns true (busy, archival in progress)  
- Includes race condition handling by double-checking for .done files
- Performs a final check to see if the WAL file itself has been removed by checkpoint process
- If the WAL file no longer exists (ENOENT), assumes it was already archived and returns false
- Returns true only if the file exists but has no status indicators

This function is almost the inverse of XLogArchiveCheckDone but differs in that it doesn't create .ready files and handles the case where files may have been already deleted.

## Parameters / Member Variables
- : The name of the XLOG segment file to check for archival status

## Dependencies
- Functions called/Symbols referenced:
  - StatusFilePath
  - XLOGDIR
- Called from (representative examples):
  - [do_pg_backup_stop](../d/do_pg_backup_stop.md)

## Notes and Other Information
- Handles race conditions between archiver completion and checkpoint deletion
- Does not recreate .ready files unlike XLogArchiveCheckDone
- Critical for backup operations to ensure all required WAL files are archived
- The function assumes that missing WAL files have been successfully archived and deleted
- Used primarily in backup completion logic to verify archival state