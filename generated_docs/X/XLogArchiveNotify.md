# XLogArchiveNotify

## Location
[src/backend/access/transam/xlogarchive.c:444-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogarchive.c#L444-L491)

## Overview
Creates an archive notification file (.ready) to signal the archiver process that a WAL file is ready for archival to long-term storage.

## Definition


## Detailed Description
XLogArchiveNotify is a core component of PostgreSQL's WAL archiving system that creates notification files to communicate with the archiver process. When a WAL file is complete and ready for archival, this function creates a corresponding .ready file in the archive_status directory.

The archiver process monitors the archive_status directory for .ready files, which serve as signals indicating which WAL files should be archived. Once the archiver successfully archives a file, it renames the .ready file to .done to indicate completion.

The function includes special handling for timeline history files, which receive the highest archival priority. For these critical files, it forces an immediate directory scan to ensure they are archived as quickly as possible, reducing the risk of timeline conflicts during standby promotion.

## Parameters / Member Variables
- : The name of the WAL file to create an archive notification for (without directory path)

## Dependencies
- Functions called/Symbols referenced:
  - StatusFilePath: Constructs the path for archive status files
  - AllocateFile: Opens a file for writing 
  - FreeFile: Closes and flushes the file
  - IsTLHistoryFileName: Checks if the file is a timeline history file
  - [PgArchForceDirScan](../P/PgArchForceDirScan.md): Forces immediate archiver directory scan for timeline history files
  - [PgArchWakeup](../P/PgArchWakeup.md): Wakes up the archiver process when running under postmaster
- Called from (representative examples):
  - [XLogArchiveNotifySeg](XLogArchiveNotifySeg.md): For individual WAL segment archival notifications
  - [writeTimeLineHistory](../w/writeTimeLineHistory.md): When creating timeline history files
  - [KeepFileRestoredFromArchive](../K/KeepFileRestoredFromArchive.md): After restoring files from archive
  - [WalReceiverMain](../W/WalReceiverMain.md): During WAL reception in streaming replication

## Notes and Other Information
- Creates empty .ready files as notifications - the file content is not significant, only its existence
- Timeline history files receive special priority treatment to prevent timeline conflicts during standby promotion
- The archiver process is responsible for renaming .ready files to .done after successful archival
- Only wakes up the archiver process when running under postmaster (not in standalone mode)
- Critical for the continuous archiving feature that enables point-in-time recovery
- The .ready/.done mechanism provides a reliable way to track archival status and prevent duplicate archival attempts
- Errors in creating notification files are logged but not fatal, allowing the system to continue operation