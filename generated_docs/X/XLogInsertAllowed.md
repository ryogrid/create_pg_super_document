# XLogInsertAllowed

## Location
[src/backend/access/transam/xlog.c:6368-6400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6368-L6400)

## Overview
XLogInsertAllowed determines whether the current process is permitted to insert new WAL (Write-Ahead Log) records into the transaction log.

## Definition

```c
bool
XLogInsertAllowed(void)
```
## Detailed Description
XLogInsertAllowed is a critical function that controls access to WAL insertion operations. It provides a fast-path mechanism for determining write permissions during different database states. The function first checks a local cache variable (LocalXLogInsertAllowed) for unconditional true/false values to avoid repeated expensive checks. If the cached value is indeterminate, it queries the recovery state via RecoveryInProgress(). Once recovery completes, the function optimizes future calls by caching the result as "unconditionally true".

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (to check if system is still recovering)
  - LocalXLogInsertAllowed (local static variable for caching)
- Called from (representative examples):
  - [XLogInsertRecord](XLogInsertRecord.md)
  - [XLogFlush](XLogFlush.md)
  - [XLogBeginInsert](XLogBeginInsert.md)
  - [WALAvailability](../W/WALAvailability.md)

## Notes and Other Information
- Ordinarily equivalent to !RecoveryInProgress() but with process-specific overrides
- Uses LocalXLogInsertAllowed as a performance optimization to avoid repeated recovery state checks
- Sets LocalXLogInsertAllowed to 1 (unconditionally true) when recovery exits for future fast-path access
- Essential for WAL write permission control across the PostgreSQL system
- Located in src/backend/access/transam/xlog.c:6368-6400
- Performance-critical function called frequently during normal database operations