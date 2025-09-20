# checkTimeLineSwitch

## Location
[src/backend/access/transam/xlogrecovery.c:2377-2425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L2377-L2425)

## Overview
A validation function that ensures timeline switches during WAL recovery are safe and consistent, preventing corruption by verifying timeline constraints and minimum recovery point requirements.

## Definition

```c
static void
checkTimeLineSwitch(XLogRecPtr lsn, TimeLineID newTLI, TimeLineID prevTLI,
					TimeLineID replayTLI)
```
## Detailed Description
This function performs critical validation when PostgreSQL encounters a timeline switch during WAL recovery, specifically at shutdown checkpoint records. Timeline switches occur in scenarios like failover, point-in-time recovery (PITR), or when following a different recovery path.

The function enforces three key safety constraints:

1. **Timeline Consistency**: Verifies that the previous timeline ID in the checkpoint record matches the currently active replay timeline, ensuring the recovery path is consistent.

2. **Timeline History Validation**: Ensures the new timeline is valid according to the expected timeline history () and that timeline IDs don't decrease, which would indicate an invalid recovery scenario.

3. **Minimum Recovery Point Protection**: Prevents switching to a timeline that would make it impossible to reach the minimum recovery point, which is crucial for maintaining data consistency in scenarios like standby promotion or PITR operations.

If any of these constraints are violated, the function calls  to immediately halt the database, as continuing would risk data corruption.

## Parameters / Member Variables
- : The Log Sequence Number of the shutdown checkpoint record being processed
- : The new timeline ID that the system is attempting to switch to
- : The previous timeline ID as recorded in the checkpoint record
- : The timeline ID currently being replayed during recovery

## Dependencies
- Functions called/Symbols referenced:
  -  (reports errors with specified severity level)
  -  (checks if timeline exists in expected timeline history)
  -  (checks if LSN is invalid)
  -  (macro for LSN formatting in error messages)
  -  (error level constant for system-wide halt)
- Called from (representative examples):
  -  (src/backend/access/transam/xlogrecovery.c:1958)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the xlogrecovery.c file
- Timeline switches can only occur at shutdown checkpoint records in PostgreSQL
- The function uses global variables , , and  for validation
- PANIC-level errors cause an immediate database shutdown and require manual intervention
- This function is essential for preventing data corruption in complex recovery scenarios
- Timeline branching typically occurs during failover situations or when performing point-in-time recovery
- The minimum recovery point constraint is particularly important for maintaining consistency in standby servers
- Timeline history validation helps prevent recovery down invalid or corrupted timeline branches