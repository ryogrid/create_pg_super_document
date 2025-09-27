# pgstat_reset_wait_event_storage

## Location
[src/backend/utils/activity/wait_event.c:362-373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L362-L373)

## Overview
Resets wait event storage location back to a local variable, typically called during backend shutdown.

## Definition
```c
void pgstat_reset_wait_event_storage(void)
```

## Detailed Description
This function resets the wait event reporting storage location by setting the global my_wait_event_info pointer back to the address of local_my_wait_event_info. This is typically called during backend shutdown to prevent the wait event reporting mechanism from trying to access shared memory locations that may become invalid. It effectively switches from shared memory storage back to local process storage for wait event information.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - None (simple assignment operation)
- Global variables accessed:
  - my_wait_event_info (modified)
  - local_my_wait_event_info (referenced)
- Called from (representative examples):
  - [ProcKill](../P/ProcKill.md) (in proc.c:915)
  - [AuxiliaryProcKill](../A/AuxiliaryProcKill.md) (in proc.c:996)

## Notes and Other Information
- Expected to be called during backend shutdown phase
- Serves as the cleanup counterpart to pgstat_set_wait_event_storage()
- Prevents access to potentially invalid shared memory locations after shutdown
- Switches wait event storage from shared memory back to local process memory
- The function performs a simple pointer assignment to local_my_wait_event_info
- Part of PostgreSQL's graceful shutdown and cleanup procedures
- Located at src/backend/utils/activity/wait_event.c:362-373

## Simplified Source

```c
// Simplified version of pgstat_reset_wait_event_storage
void pgstat_reset_wait_event_storage(void) {
    // Reset wait event storage back to local variable
    // This switches from shared memory storage to local process storage
    my_wait_event_info = &local_my_wait_event_info;
}
```

Key simplifications made:
- Added explanatory comments for the single operation
- Function is already very simple - only contains one assignment statement
- Preserved the essential logic: resetting the global pointer to local storage
- Focused on the core purpose: switching from shared to local storage during shutdown