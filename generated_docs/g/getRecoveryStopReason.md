# getRecoveryStopReason

## Location
[src/backend/access/transam/xlogrecovery.c:2886-2924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L2886-L2924)

## Overview
Creates a human-readable comment explaining why and where recovery stopped for inclusion in the timeline history file.

## Definition
```c
static char *getRecoveryStopReason(void)
```

## Detailed Description
The `getRecoveryStopReason` function generates descriptive text that explains the reason recovery was stopped, which is later recorded in PostgreSQL's timeline history files. These history files serve as a permanent record of recovery operations and timeline changes, providing administrators with crucial information about database recovery events.

The function examines the global recovery target settings and recovery stop information to construct an appropriate message that describes whether recovery stopped before or after a specific transaction, at a particular timestamp, at a specific LSN position, at a named restore point, or upon reaching consistency. This information is essential for database administration, auditing, and troubleshooting recovery operations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - snprintf: Formats the reason string based on recovery target type
  - [timestamptz_to_str](../t/timestamptz_to_str.md): Converts timestamp to human-readable string
  - [pstrdup](../p/pstrdup.md): Creates a palloc'd copy of the reason string
- Constants used:
  - RECOVERY_TARGET_XID: Recovery target type for specific transaction ID
  - RECOVERY_TARGET_TIME: Recovery target type for specific timestamp
  - RECOVERY_TARGET_LSN: Recovery target type for specific LSN
  - RECOVERY_TARGET_NAME: Recovery target type for named restore points
  - RECOVERY_TARGET_IMMEDIATE: Recovery target type for immediate stop
  - LSN_FORMAT_ARGS: Macro for formatting LSN values
- Global variables accessed:
  - recoveryTarget: The type of recovery target that was set
  - recoveryStopAfter: Whether recovery stopped after (true) or before (false) the target
  - recoveryStopXid: Transaction ID where recovery stopped (for XID targets)
  - recoveryStopTime: Timestamp where recovery stopped (for time targets)
  - recoveryStopLSN: LSN position where recovery stopped (for LSN targets)
  - recoveryStopName: Name of restore point where recovery stopped (for name targets)
- Called from:
  - [FinishWalRecovery](../F/FinishWalRecovery.md): Uses this to generate timeline history entries

## Notes and Other Information
- This is a static function, only accessible within xlogrecovery.c
- Returns a dynamically allocated string that must be freed by the caller
- The returned string is designed for inclusion in timeline history files
- Provides different message formats depending on the recovery target type:
  - XID targets: "before/after transaction [id]"
  - Time targets: "before/after [timestamp]"
  - LSN targets: "before/after LSN [position]"
  - Named targets: "at restore point [name]"
  - Immediate targets: "reached consistency"
  - Default case: "no recovery target specified"
- Critical for database administration and recovery auditing
- The generated text becomes part of PostgreSQL's permanent timeline history records
- Used for documenting recovery operations in timeline history files