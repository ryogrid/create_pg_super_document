# WaitEventAdjustKqueue

## Location
src/backend/storage/ipc/latch.c: 1263 - 1368

## Overview
A static function that manages kqueue event registration for a WaitEvent by computing differences between old and new event masks and making appropriate kevent system calls to add, modify, or delete event monitoring.

## Definition
```c
static void
WaitEventAdjustKqueue(WaitEventSet *set, WaitEvent *event, int old_events)
```

## Detailed Description
This function serves as the core kqueue event management routine in PostgreSQL's event waiting system. It compares the old event mask with the new event mask to determine what changes need to be made to the kqueue monitoring setup. The function handles three main types of events: postmaster death monitoring (using process events), latch signaling (using signal events), and socket I/O events (using read/write filters). For socket events, it carefully manages separate EVFILT_READ and EVFILT_WRITE filters, computing the necessary adds and deletes to transition from the old state to the new state. The function includes special error handling for postmaster death monitoring, accounting for race conditions where the postmaster might have already exited.

## Parameters / Member Variables
- `set`: Pointer to the WaitEventSet that contains the kqueue file descriptor and manages the event collection
- `event`: Pointer to the WaitEvent structure that holds the file descriptor and event mask to be monitored
- `old_events`: The previous event mask, used to compute what has changed from the last call

## Dependencies
- Functions called/Symbols referenced:
  - WaitEventAdjustKqueueAddPostmaster (for postmaster death events)
  - WaitEventAdjustKqueueAddLatch (for latch events)
  - WaitEventAdjustKqueueAdd (for socket I/O events, called multiple times)
  - kevent (system call to modify kqueue)
  - PostmasterIsAlive (to check postmaster status)
  - errcode_for_socket_access (for error reporting)
  - getppid (to verify parent process)
  - ereport (for error reporting)
- Called from (representative examples):
  - LatchWaitSetLatchPos (at line 190)
  - AddWaitEventToSet (at line 1031) 
  - ModifyWaitEvent (at line 1111)

## Notes and Other Information
- Handles up to 2 kevent structures simultaneously (for read and write events on the same socket)
- Includes sophisticated error handling for postmaster death detection, considering race conditions and debugger interference
- Uses boolean flags to track old and new filter states to minimize unnecessary kevent calls
- Sets report_postmaster_not_running flag when postmaster death is detected or cannot be monitored
- Part of the kqueue-specific implementation of PostgreSQL's cross-platform event waiting infrastructure
- Optimizes for the common case where no changes are needed (early return when old_events == event->events)
- Essential for efficient I/O multiplexing on BSD-derived systems including macOS and FreeBSD