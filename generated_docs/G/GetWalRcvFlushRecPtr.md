# GetWalRcvFlushRecPtr

## Location
src/backend/replication/walreceiverfuncs.c: 331 - 351

## Overview
Returns the last+1 byte position that the WAL receiver has successfully flushed to disk, with optional retrieval of additional chunk and timeline information.

## Definition


## Detailed Description
This function provides a thread-safe way to query the current flush position of the WAL receiver process. It accesses the shared WalRcvData structure to retrieve the flushed position (flushedUpto), which represents the last byte that has been safely written to persistent storage. The function also optionally returns the start position of the most recent chunk that was flushed and the timeline ID being received. This information is crucial for recovery processes and replication monitoring.

## Parameters / Member Variables
- `latestChunkStart`: Output parameter for the first byte position of the most recent flush cycle (optional, can be NULL)
- `receiveTLI`: Output parameter for the timeline ID currently being received (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - WalRcvData (shared memory structure)
  - SpinLockAcquire/SpinLockRelease (for mutex protection)
- Called from (representative examples):
  - CreateRestartPoint (during checkpoint creation)
  - pg_last_wal_receive_lsn (SQL function)
  - WaitForWALToBecomeAvailable (during recovery)
  - GetStandbyFlushRecPtr (in walsender)

## Notes and Other Information
- Thread-safe through spinlock protection of the walrcv mutex
- Returns InvalidXLogRecPtr equivalent if no WAL has been flushed yet
- Essential for coordinating between recovery processes and WAL receiver
- The flush position represents data durably stored, not just received in memory
- Used extensively by checkpointing, recovery, and replication lag monitoring
- Located in src/backend/replication/walreceiverfuncs.c:331-351