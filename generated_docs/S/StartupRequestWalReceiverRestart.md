# StartupRequestWalReceiverRestart

## Location
[src/backend/access/transam/xlogrecovery.c:4376-4394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4376-L4394)

## Overview
Signals the WAL receiver process to restart when called by the startup process upon detecting relevant configuration changes.

## Definition
```c
void StartupRequestWalReceiverRestart(void)
```

## Detailed Description
This function provides a mechanism for the startup process to request a restart of the WAL receiver process during streaming replication. It is typically called when configuration changes occur that require the WAL receiver to be restarted with new settings.

The function performs a safety check to ensure that:
1. The current WAL source is streaming replication (XLOG_FROM_STREAM)
2. The WAL receiver process is actually running

If both conditions are met, it sets the `pendingWalRcvRestart` flag to true and logs an informational message. This flag is later checked by other parts of the recovery system to perform the actual restart operation.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvRunning](../W/WalRcvRunning.md)
  - XLOG_FROM_STREAM (constant)
  - ereport/LOG (logging)
- Called from (representative examples):
  - [StartupRereadConfig](StartupRereadConfig.md)
  - [EndOfWalRecoveryInfo](../E/EndOfWalRecoveryInfo.md) (header reference)

## Notes and Other Information
- This is a public function (not static), accessible from other modules
- Modifies the global variable `pendingWalRcvRestart` and `currentSource` (read-only access)
- Only operates when currently receiving WAL via streaming replication
- The actual restart is handled by other parts of the system that monitor the `pendingWalRcvRestart` flag
- Logs a message at LOG level to indicate the restart request
- Safe to call even when streaming replication is not active (no-op in that case)
- Part of PostgreSQL's streaming replication infrastructure for handling dynamic configuration changes