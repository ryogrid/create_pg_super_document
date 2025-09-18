# SetTransactionIdLimit

## Location
src/backend/access/transam/varsup.c: 372 - 516

## Overview
Determines and sets various transaction ID limits for wraparound prevention based on the oldest database frozen XID, configuring autovacuum triggers and warning thresholds to prevent transaction ID wraparound.

## Definition
```c
void SetTransactionIdLimit(TransactionId oldest_datfrozenxid, Oid oldest_datoid)
```

## Detailed Description
This critical function establishes multiple transaction ID limits that govern PostgreSQL's transaction wraparound prevention mechanisms. It calculates four key thresholds based on the oldest frozen transaction ID across all databases:

1. **xidVacLimit**: Triggers automatic vacuum when reached (based on `autovacuum_freeze_max_age`)
2. **xidWarnLimit**: Issues warnings when reached (40M transactions before wraparound)
3. **xidStopLimit**: Stops new transaction assignment in interactive mode (3M transactions before wraparound)
4. **xidWrapLimit**: The absolute wraparound point (halfway around from oldest XID)

The function implements PostgreSQL's multi-layered defense against transaction ID wraparound, providing ample warning and automatic remediation before reaching critical points. It immediately signals autovacuum if past the vacuum limit and issues warnings if past the warn limit.

The limits are calculated with generous safety margins to account for scenarios where automatic clients might not respond to warnings, and to provide DBAs with sufficient room for manual intervention in standalone mode.

## Parameters / Member Variables
- `oldest_datfrozenxid`: The oldest frozen transaction ID among all databases in the cluster, used as the baseline for calculating all limits
- `oldest_datoid`: The OID of the database containing the oldest frozen XID, used for error reporting and diagnostics

## Dependencies
- Functions called/Symbols referenced:
  - `TransactionIdIsNormal`
  - `LWLockAcquire` (XidGenLock, LW_EXCLUSIVE)
  - `XidFromFullTransactionId`
  - `LWLockRelease` (XidGenLock)
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md)
  - `SendPostmasterSignal` (PMSIGNAL_START_AUTOVAC_LAUNCHER)
  - [IsTransactionState](../I/IsTransactionState.md)
  - [get_database_name](../g/get_database_name.md)
- Called from (representative examples):
  - [BootStrapXLOG](../B/BootStrapXLOG.md) (src/backend/access/transam/xlog.c:5056)
  - [StartupXLOG](StartupXLOG.md) (src/backend/access/transam/xlog.c:5532)
  - [xlog_redo](../x/xlog_redo.md) (src/backend/access/transam/xlog.c:8304, 8415)
  - [vac_truncate_clog](../v/vac_truncate_clog.md) (src/backend/commands/vacuum.c:1946)

## Notes and Other Information
- The function uses hardcoded safety margins: 40M XIDs for warnings, 3M XIDs for stopping new transactions
- Automatically triggers autovacuum launcher when past the vacuum limit to ensure continuous protection
- Provides detailed warning messages with hints about how to resolve wraparound issues
- The `autovacuum_freeze_max_age` parameter is validated by guc.c to ensure sane limits
- All limits handle XID wraparound arithmetic correctly using modular comparison functions
- Called during recovery, normal operation, and after significant vacuum operations to maintain current limits