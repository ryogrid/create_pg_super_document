# InitializeLatchWaitSet

## Location
[src/backend/storage/ipc/latch.c:346-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L346-L363)

## Overview
Initializes the global LatchWaitSet used by WaitLatch() function for efficient latch waiting across PostgreSQL processes.

## Definition
```c
void
InitializeLatchWaitSet(void)
```

## Detailed Description
This function sets up the global LatchWaitSet, which is a pre-configured WaitEventSet used by the WaitLatch() function for efficient waiting on latch events. The function creates a WaitEventSet with capacity for 2 events and adds the process's own latch (MyLatch) as a WL_LATCH_SET event. For processes running under the postmaster, it also adds a WL_EXIT_ON_PM_DEATH event to ensure the process exits if the postmaster dies unexpectedly. The function uses assertions to verify that LatchWaitSet is not already initialized and that the latch position matches the expected LatchWaitSetLatchPos constant.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)
  - [AddWaitEventToSet](../A/AddWaitEventToSet.md)
  - MyLatch (global variable)
  - LatchWaitSet (global variable)
  - LatchWaitSetLatchPos (constant)
  - WL_LATCH_SET (event type)
  - WL_EXIT_ON_PM_DEATH (event type)
  - PGINVALID_SOCKET (constant)
  - IsUnderPostmaster (global variable)
- Called from (representative examples):
  - [InitPostmasterChild](InitPostmasterChild.md)
  - [InitStandaloneProcess](InitStandaloneProcess.md)

## Notes and Other Information
- Located in src/backend/storage/ipc/latch.c:346-363
- Must be called after InitializeLatchSupport() during process initialization
- Creates a shared WaitEventSet that can be reused by WaitLatch() calls
- Automatically adds postmaster death detection for child processes
- Uses PG_USED_FOR_ASSERTS_ONLY to mark variables only used in assertions
- The LatchWaitSet is a global resource that improves performance by avoiding repeated WaitEventSet creation
- Essential for PostgreSQL's efficient process coordination and cleanup mechanisms

## Simplified Source

```c
// Simplified version of InitializeLatchWaitSet
void InitializeLatchWaitSet(void) {
    int latch_pos;

    // Ensure we haven't already initialized the wait set
    Assert(LatchWaitSet == NULL);

    // Create a wait event set for latch operations
    LatchWaitSet = CreateWaitEventSet(NULL, 2);

    // Add our own latch to the wait set
    latch_pos = AddWaitEventToSet(LatchWaitSet, WL_LATCH_SET, PGINVALID_SOCKET,
                                  MyLatch, NULL);

    // For child processes, also watch for postmaster death
    if (IsUnderPostmaster)
        AddWaitEventToSet(LatchWaitSet, WL_EXIT_ON_PM_DEATH,
                          PGINVALID_SOCKET, NULL, NULL);

    // Verify the latch was added at the expected position
    Assert(latch_pos == LatchWaitSetLatchPos);
}
```

Key simplifications made:
- Removed debug-only variable annotation for clarity
- Added descriptive comments for each major operation
- Preserved all essential logic and assertions
- Maintained the process safety mechanism for postmaster death detection
- Focused on the core purpose: setting up efficient latch waiting infrastructure