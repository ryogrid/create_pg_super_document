# XLogFileClose

## Location
[src/backend/access/transam/xlog.c:3616-3666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3616-L3666)

## Overview
Closes the currently open XLOG segment file for writing, performing cache management optimizations and proper cleanup of file descriptor resources.

## Definition
static void XLogFileClose(void)

## Detailed Description
XLogFileClose is a static function responsible for properly closing the currently open WAL segment file. The function performs intelligent cache management by advising the operating system to release cached pages when WAL archiving or streaming is not active, as these files typically won't be re-read during normal operation. However, it preserves cache when archiver or walsender processes might need to read the segment.

The function includes comprehensive error handling with PANIC-level reporting for close failures, reflecting the critical nature of WAL file operations. Upon successful closure, it resets the global openLogFile variable and releases the external file descriptor to maintain proper resource accounting.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - XLogIsNeeded
  - posix_fadvise (conditional on USE_POSIX_FADVISE)
  - close
  - [XLogFileName](XLogFileName.md)  
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
  - [ReleaseExternalFD](../R/ReleaseExternalFD.md)
- Called from (representative examples):
  - RefreshXLogWriteResult (src/backend/access/transam/xlog.c:680)
  - [XLogWrite](XLogWrite.md) (src/backend/access/transam/xlog.c:2362, 2545)
  - [XLogBackgroundFlush](XLogBackgroundFlush.md) (src/backend/access/transam/xlog.c:3016)
  - [assign_wal_sync_method](../a/assign_wal_sync_method.md) (src/backend/access/transam/xlog.c:8686)

## Notes and Other Information
- Operates on global variables: openLogFile, openLogTLI, openLogSegNo
- Uses posix_fadvise(POSIX_FADV_DONTNEED) for cache management when available
- Cache optimization is disabled when WAL archiving/streaming is active
- Cache optimization is also disabled when IO_DIRECT_WAL is set
- Uses PANIC error level for close failures, indicating critical system failure
- Resets openLogFile to -1 after successful closure
- Calls ReleaseExternalFD() for proper resource accounting
- Assert checks that openLogFile >= 0 before attempting to close
- Located in src/backend/access/transam/xlog.c:3616-3666