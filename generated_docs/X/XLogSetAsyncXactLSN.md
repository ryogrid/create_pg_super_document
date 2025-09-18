# XLogSetAsyncXactLSN

## Location
src/backend/access/transam/xlog.c: 2614 - 2664

## Overview
Records the LSN of an asynchronous transaction commit or abort and intelligently awakens the WAL writer process when necessary to ensure timely WAL flushing.

## Definition
```c
void XLogSetAsyncXactLSN(XLogRecPtr asyncXactLSN)
```

## Detailed Description
XLogSetAsyncXactLSN is specifically designed for asynchronous transaction processing, where transactions don't wait for WAL to be flushed to disk before returning to the client. The function maintains a shared record of the highest LSN requiring asynchronous flushing and implements smart WAL writer awakening logic. It only wakes the WAL writer when it's sleeping or when there's sufficient pending WAL data to justify a flush operation, optimizing system resource usage while ensuring async transactions reach disk within reasonable time bounds.

## Parameters / Member Variables
- `asyncXactLSN`: XLogRecPtr representing the LSN of the asynchronous transaction that needs to be flushed to disk

## Dependencies
- Functions called/Symbols referenced:
  - RefreshXLogWriteResult (updates local WAL write status)
  - SetLatch (awakens the WAL writer process)
- Global variables used:
  - XLogCtl (shared WAL control structure containing asyncXactLSN and WalWriterSleeping)
  - LogwrtResult (local copy of WAL write results)
  - WalWriterFlushAfter (configuration parameter)
  - ProcGlobal->walwriterLatch (WAL writer process latch)
- Called from (representative examples):
  - RecordTransactionCommit (in xact.c:1502)
  - RecordTransactionAbort (in xact.c:1810)
  - AbortTransaction (in xact.c:2881)
  - LogCurrentRunningXacts (in standby.c:1395)

## Notes and Other Information
- Should NOT be called for synchronous commits (as documented in function comment)
- Implements an optimization to avoid redundant work: if another process already set a higher asyncXactLSN, this call returns early
- Uses a dual-strategy approach for WAL writer awakening: always wake if sleeping, or wake based on pending flush volume when active
- The flush threshold logic mirrors XLogBackgroundFlush() behavior for consistency
- Thread-safe implementation using spinlocks around shared state modifications
- Part of PostgreSQL's async commit performance optimization, allowing transactions to complete without waiting for disk I/O
- Declared in src/include/access/xlog.h at line 213