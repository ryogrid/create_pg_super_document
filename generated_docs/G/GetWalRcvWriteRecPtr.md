# GetWalRcvWriteRecPtr

## Location
[src/backend/replication/walreceiverfuncs.c:352-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiverfuncs.c#L352-L363)

## Overview
Returns the last+1 byte position that the WAL receiver has written to WAL buffers, providing a lock-free way to query the current write position.

## Definition

```c
XLogRecPtr
GetWalRcvWriteRecPtr(void)
```
## Detailed Description
This function provides a fast, lock-free method to retrieve the current write position of the WAL receiver process. Unlike GetWalRcvFlushRecPtr which returns the flushed (durable) position, this function returns the written position which may be ahead of what has been flushed to disk. It uses atomic operations to read the writtenUpto field without requiring mutex protection, making it suitable for frequent polling or monitoring scenarios where performance is critical.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvData](../W/WalRcvData.md) (shared memory structure)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md) (atomic read operation)
- Called from (representative examples):
  - Limited direct usage found in the codebase

## Notes and Other Information
- Lock-free implementation using atomic operations for high performance
- Returns data that may be more recent than the flushed position
- Useful for monitoring WAL receiver progress without blocking
- The written position represents data received and written to WAL buffers but not necessarily durably stored
- Simpler interface compared to GetWalRcvFlushRecPtr as it only returns the write position
- Located in src/backend/replication/walreceiverfuncs.c:352-363

## Simplified Source

```c
XLogRecPtr
GetWalRcvWriteRecPtr(void)
{
    WalRcvData *walrcv = WalRcv;

    // Return the last written position using atomic read
    return pg_atomic_read_u64(&walrcv->writtenUpto);
}
```