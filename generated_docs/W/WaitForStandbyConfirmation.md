# WaitForStandbyConfirmation

## Location
[src/backend/replication/slot.c:2746-2782](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L2746-L2782)

## Overview
WaitForStandbyConfirmation waits for physical standby servers to confirm receipt of WAL data up to a specified LSN, used specifically by logical decoding SQL functions to ensure data consistency across synchronized standby slots.

## Definition
```c
void WaitForStandbyConfirmation(XLogRecPtr wait_for_lsn)
```

## Detailed Description
This function implements a synchronous waiting mechanism for logical replication scenarios where the primary database needs to ensure that physical standby servers have received and processed WAL data up to a specific point before proceeding. It is specifically designed for use with logical failover slots and the synchronized_standby_slots configuration.

The function operates by entering a polling loop that periodically checks if all configured standby slots have caught up to the target LSN using StandbySlotsHaveCaughtup(). It employs a condition variable with a timeout mechanism to avoid indefinite blocking while still being responsive to configuration changes.

Early return occurs if the current replication slot is not a logical failover slot or if no synchronized_standby_slots are configured, as waiting would be unnecessary in these cases.

## Parameters / Member Variables
- `wait_for_lsn`: The target WAL location (XLogRecPtr) that all synchronized standby slots must reach before the function returns

## Dependencies
- Functions called/Symbols referenced:
  - [ConditionVariablePrepareToSleep](../C/ConditionVariablePrepareToSleep.md)
  - [ConditionVariableTimedSleep](../C/ConditionVariableTimedSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - [StandbySlotsHaveCaughtup](../S/StandbySlotsHaveCaughtup.md)
  - ProcessConfigFile
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)

## Notes and Other Information
- Only functions when MyReplicationSlot is a logical failover slot and synchronized_standby_slots is configured
- Uses a 1-second timeout in the condition variable wait to remain responsive to configuration changes
- Processes configuration file reloads during the wait loop to handle dynamic changes to synchronized_standby_slots
- Uses WARNING level for error reporting in StandbySlotsHaveCaughtup calls
- The function is interruptible via CHECK_FOR_INTERRUPTS() to handle query cancellation and shutdown signals
- Critical for maintaining consistency in logical replication with failover capabilities, ensuring standby servers are sufficiently caught up before logical decoding proceeds