# RecoveryTargetAction

## Location
[src/include/access/xlog_internal.h:327-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L327-L348)

## Overview
RecoveryTargetAction is an enumeration type that defines the possible actions to take when PostgreSQL reaches a specified recovery target during WAL (Write-Ahead Log) replay and point-in-time recovery operations.

## Definition


## Detailed Description
This enumeration controls what PostgreSQL should do when it reaches a configured recovery target during archive recovery or streaming replication. The recovery target can be specified by time, transaction ID, LSN, or other criteria. Once the target is reached, the system must decide whether to pause recovery (allowing inspection), promote to become a primary server, or shutdown gracefully.

The RecoveryTargetAction is used in conjunction with the recovery_target_action GUC (Grand Unified Configuration) parameter, which can be set in postgresql.conf or recovery configuration. This provides administrators fine-grained control over recovery behavior, which is crucial for point-in-time recovery scenarios and failover operations.

## Parameters / Member Variables
- : Pause recovery at the target point, allowing manual intervention and inspection before deciding next steps. Requires hot_standby to be enabled; if disabled, automatically converts to SHUTDOWN action.
- : Automatically promote the server to become a primary (read-write) server when the recovery target is reached, completing the recovery process.
- : Shutdown the PostgreSQL server when the recovery target is reached, typically used when manual intervention is required before promotion.

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a simple enumeration)
- Called from (representative examples):
  - recoveryTargetAction global variable in xlogrecovery.c:87
  - recovery_target_action_options configuration array in xlogrecovery.c:74-78
  - Switch statement in recovery completion logic in xlogrecovery.c:1851-1869

## Notes and Other Information
- The default action is RECOVERY_TARGET_ACTION_PAUSE, providing a safe default that allows administrators to verify recovery results before proceeding.
- When RECOVERY_TARGET_ACTION_PAUSE is specified but EnableHotStandby is false, the system automatically overrides to RECOVERY_TARGET_ACTION_SHUTDOWN for consistency (xlogrecovery.c:1139-1141).
- This enumeration is part of PostgreSQL's comprehensive point-in-time recovery (PITR) system, essential for backup and disaster recovery scenarios.
- The action is configured via the recovery_target_action GUC parameter with string values: "pause", "promote", or "shutdown".
- Located in src/include/access/xlog_internal.h:322-327, this is an internal header not exposed to client applications.