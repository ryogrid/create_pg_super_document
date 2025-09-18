# XLogArchiveForceDone

## Location
src/backend/access/transam/xlogarchive.c: 510 - 564

## Overview
Forces the creation of an archive completion notification file (.done) to indicate that a WAL segment has been successfully archived, bypassing the normal archival process.

## Definition
void XLogArchiveForceDone(const char *xlog)

## Detailed Description
XLogArchiveForceDone forcibly creates a .done status file for a given WAL segment file, indicating that the segment has been successfully archived. This function bypasses the normal archive process and creates the completion notification regardless of whether a .ready file exists. 

The function first checks if a .done file already exists and exits early if found. If a .ready file exists, it renames it to .done using durable_rename for atomic operation. If no .ready file exists, it creates an empty .done file directly. This forced completion is typically used in recovery scenarios or when manually managing archive states.

## Parameters / Member Variables
- : The name of the XLOG segment file for which to create the archive done notification

## Dependencies
- Functions called/Symbols referenced:
  - StatusFilePath
  - [durable_rename](../d/durable_rename.md)  
  - AllocateFile
  - FreeFile
- Called from (representative examples):
  - [KeepFileRestoredFromArchive](../K/KeepFileRestoredFromArchive.md)
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [WalRcvFetchTimeLineHistoryFiles](../W/WalRcvFetchTimeLineHistoryFiles.md)
  - [XLogWalRcvClose](XLogWalRcvClose.md)

## Notes and Other Information
- Creates .done files in the archive_status directory under pg_wal
- Uses durable_rename for atomic file operations when a .ready file exists
- Logs warnings but continues execution if file operations fail
- Primarily used during WAL recovery and replication processes
- The function ensures idempotent behavior by checking for existing .done files