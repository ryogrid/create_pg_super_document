# NeedToWaitForStandbys

## Location
[src/backend/replication/walsender.c:1762-1793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L1762-L1793)

## Overview
NeedToWaitForStandbys determines whether a logical failover slot should wait for standby servers to catch up to a specified LSN position before proceeding.

## Definition
```c
static bool NeedToWaitForStandbys(XLogRecPtr flushed_lsn, uint32 *wait_event)
```

## Detailed Description
This function implements a critical component of PostgreSQL`s logical replication failover mechanism. It checks whether the current process is using a logical failover slot and whether all configured standby slots have caught up to the specified `flushed_lsn` position. The function is designed to ensure data consistency during failover scenarios by preventing the primary from advancing too far ahead of standbys.

The function handles shutdown scenarios gracefully by escalating the error level from WARNING to ERROR when a shutdown signal has been received (`got_STOPPING`), preventing indefinite waits during shutdown. It sets an appropriate wait event when waiting is required to integrate properly with PostgreSQL`s wait event infrastructure.

## Parameters / Member Variables
- `flushed_lsn`: XLogRecPtr representing the LSN position that standbys should have reached
- `wait_event`: Pointer to uint32 that receives the wait event type (WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION when waiting is needed, 0 otherwise)

## Dependencies
- Functions called/Symbols referenced:
  - StandbySlotsHaveCaughtup (checks if all standby slots have reached the specified LSN)
- Global variables accessed:
  - got_STOPPING (indicates if shutdown signal was received)
  - replication_active (indicates if replication is currently active)
  - MyReplicationSlot (current process replication slot)
- Called from (representative examples):
  - [NeedToWaitForWal](NeedToWaitForWal.md) (higher-level wait logic coordinator)
  - [WalSndWaitForWal](../W/WalSndWaitForWal.md) (main WAL sender wait loop)

## Notes and Other Information
- Only applies to logical failover slots (`MyReplicationSlot->data.failover` must be true)
- Requires active replication (`replication_active` must be true) to trigger waiting behavior
- Uses escalating error levels during shutdown to prevent indefinite waits
- Integrates with PostgreSQL`s wait event system for monitoring and debugging
- Essential for maintaining consistency in logical replication failover scenarios
- Part of the broader mechanism that ensures logical replication can survive primary server failures
- The function returns false immediately if not dealing with an active logical failover slot