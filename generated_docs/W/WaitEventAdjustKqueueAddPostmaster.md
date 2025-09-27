# WaitEventAdjustKqueueAddPostmaster

## Location
[src/backend/storage/ipc/latch.c:1236-1247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L1236-L1247)

## Overview
A static inline function that configures a kevent structure to monitor the postmaster process for termination events using the kqueue EVFILT_PROC filter.

## Definition
```c
static inline void
WaitEventAdjustKqueueAddPostmaster(struct kevent *k_ev, WaitEvent *event)
```

## Detailed Description
This function sets up kqueue-based monitoring for postmaster process death detection. It configures a kevent structure to watch for the postmaster process exit using the EVFILT_PROC filter with NOTE_EXIT flags. The function is specifically designed for adding postmaster death monitoring and currently does not support removal of such monitoring. This is part of PostgreSQL's mechanism to detect when the postmaster process has terminated, which is critical for proper cleanup and shutdown procedures in child processes.

## Parameters / Member Variables
- `k_ev`: Pointer to the kevent structure to be configured for postmaster monitoring
- `event`: Pointer to the WaitEvent structure that will be associated with this kevent

## Dependencies
- Functions called/Symbols referenced:
  - PostmasterPid (global variable containing the postmaster process ID)
  - AccessWaitEvent (macro for associating WaitEvent with kevent)
  - [WaitEvent](WaitEvent.md) (structure type)
  - EVFILT_PROC (kqueue filter for process events)
  - EV_ADD (kqueue flag to add the event)
  - NOTE_EXIT (kqueue flag to monitor process exit)
- Called from (representative examples):
  - [WaitEventAdjustKqueue](WaitEventAdjustKqueue.md) (at line 1290)

## Notes and Other Information
- This function only supports adding postmaster death monitoring, not removing it, as indicated by the comment
- Uses EVFILT_PROC filter which is specific to process-related events in kqueue
- The NOTE_EXIT flag specifically monitors for process termination
- Part of PostgreSQL's child process management and cleanup infrastructure
- The PostmasterPid is used as the identifier to monitor the specific postmaster process
- Essential for ensuring child processes can detect and respond to postmaster termination

## Simplified Source

```c
// Simplified version of WaitEventAdjustKqueueAddPostmaster
static inline void WaitEventAdjustKqueueAddPostmaster(struct kevent *k_ev, WaitEvent *event) {
    // Configure kevent to monitor postmaster process termination
    k_ev->ident = PostmasterPid;        // Process ID to monitor
    k_ev->filter = EVFILT_PROC;         // Monitor process events
    k_ev->flags = EV_ADD;               // Add this event to kqueue monitoring
    k_ev->fflags = NOTE_EXIT;           // Specifically watch for process exit
    k_ev->data = 0;                     // No additional data needed

    // Associate the kevent with our WaitEvent structure
    AccessWaitEvent(k_ev) = event;

    // Note: For now postmaster death can only be added, not removed
}
```

Key simplifications made:
- Added inline comments explaining each field's purpose in process monitoring
- Clarified that this specifically monitors for postmaster process termination
- Maintained the exact same logic as this is already a simple utility function
- Preserved the comment about add-only functionality
- Explained the kqueue-specific constants used for process monitoring