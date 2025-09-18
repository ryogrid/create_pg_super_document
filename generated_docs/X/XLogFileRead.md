# XLogFileRead

## Location
[src/backend/access/transam/xlogrecovery.c:4192-4273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4192-L4273)

## Overview
Opens a WAL (Write-Ahead Log) segment file for reading during recovery, handling both archival and local storage sources.

## Definition
```c
static int XLogFileRead(XLogSegNo segno, int emode, TimeLineID tli, XLogSource source, bool notfoundOk)
```

## Detailed Description
This function opens a specific WAL segment file for reading during database recovery operations. It handles different sources of WAL data including archived files and files already present in the pg_wal directory. The function performs different operations based on the source:

For XLOG_FROM_ARCHIVE: Retrieves the segment from archival storage using `RestoreArchivedFile`, then moves it to the proper location in pg_wal using `KeepFileRestoredFromArchive`.

For XLOG_FROM_PG_WAL and XLOG_FROM_STREAM: Constructs the file path directly and opens the existing file.

The function updates various state variables to track the source of data and sets process status information for monitoring recovery progress. It returns a file descriptor on success or -1 on failure.

## Parameters / Member Variables
- `segno`: The WAL segment number to read
- `emode`: Error mode for handling failures (though not directly used in current implementation)
- `tli`: Timeline ID for the WAL segment
- `source`: Source location of the WAL file (XLOG_FROM_ARCHIVE, XLOG_FROM_PG_WAL, or XLOG_FROM_STREAM)
- `notfoundOk`: Whether it's acceptable for the file to not exist (returns -1 instead of panicking)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogFileName](XLogFileName.md)
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md)
  - [KeepFileRestoredFromArchive](../K/KeepFileRestoredFromArchive.md)
  - XLogFilePath
  - BasicOpenFile
  - set_ps_display
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [IsInstallXLogFileSegmentActive](../I/IsInstallXLogFileSegmentActive.md)
- Called from (representative examples):
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md)
  - [XLogFileReadAnyTLI](XLogFileReadAnyTLI.md)

## Notes and Other Information
- This is a static function, only accessible within the xlogrecovery.c module
- Updates global state variables including `curFileTLI`, `readSource`, `XLogReceiptSource`, and `XLogReceiptTime`
- Sets process status display messages to show recovery progress
- For archived files, the function ensures the file is properly installed in pg_wal before opening
- Returns a valid file descriptor on success, or -1 on acceptable failures
- Panics on unexpected failures unless `notfoundOk` is true and the error is ENOENT (file not found)