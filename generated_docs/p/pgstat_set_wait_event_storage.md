# pgstat_set_wait_event_storage

## Location
[src/backend/utils/activity/wait_event.c:350-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L350-L361)

## Overview
Configures wait event reporting by setting the storage location where wait event information will be reported.

## Definition
```c
void pgstat_set_wait_event_storage(uint32 *wait_event_info)
```

## Detailed Description
This function establishes the storage location for wait event reporting by setting the global my_wait_event_info pointer to the provided wait_event_info address. This is typically called during backend startup to point the wait event reporting mechanism to a location in shared memory where wait event information can be stored and accessed by other processes. The provided storage must remain valid until pgstat_reset_wait_event_storage() is called.

## Parameters / Member Variables
- `wait_event_info`: Pointer to a 32-bit unsigned integer where wait event information will be stored. This memory location must remain valid until reset.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple assignment operation)
- Global variables modified:
  - my_wait_event_info
- Called from (representative examples):
  - [InitProcess](../I/InitProcess.md) (in proc.c:453)
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md) (in proc.c:620)

## Notes and Other Information
- Expected to be called during backend startup phase
- The function performs a simple pointer assignment to configure wait event storage
- The provided storage location is typically in shared memory for inter-process access
- Must be paired with pgstat_reset_wait_event_storage() to properly clean up
- This is part of PostgreSQL's statistics and monitoring infrastructure
- Located at src/backend/utils/activity/wait_event.c:350-361

## Simplified Source

```c
// Simplified version of pgstat_set_wait_event_storage
void pgstat_set_wait_event_storage(uint32 *wait_event_info) {
    // Configure global wait event storage pointer to point to provided location
    // This connects the wait event reporting system to shared memory
    my_wait_event_info = wait_event_info;
}
```

Key simplifications made:
- Function is already very simple with just one assignment
- Added explanatory comments to clarify the purpose
- Focused on the core functionality: configuring wait event storage location