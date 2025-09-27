# WaitEventAdjustKqueue

## Location
[src/backend/storage/ipc/latch.c:1263-1368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L1263-L1368)

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
  - [WaitEventAdjustKqueueAddPostmaster](WaitEventAdjustKqueueAddPostmaster.md) (for postmaster death events)
  - [WaitEventAdjustKqueueAddLatch](WaitEventAdjustKqueueAddLatch.md) (for latch events)
  - [WaitEventAdjustKqueueAdd](WaitEventAdjustKqueueAdd.md) (for socket I/O events, called multiple times)
  - kevent (system call to modify kqueue)
  - [PostmasterIsAlive](../P/PostmasterIsAlive.md) (to check postmaster status)
  - [errcode_for_socket_access](../e/errcode_for_socket_access.md) (for error reporting)
  - getppid (to verify parent process)
  - ereport (for error reporting)
- Called from (representative examples):
  - LatchWaitSetLatchPos (at line 190)
  - [AddWaitEventToSet](../A/AddWaitEventToSet.md) (at line 1031) 
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md) (at line 1111)

## Notes and Other Information
- Handles up to 2 kevent structures simultaneously (for read and write events on the same socket)
- Includes sophisticated error handling for postmaster death detection, considering race conditions and debugger interference
- Uses boolean flags to track old and new filter states to minimize unnecessary kevent calls
- Sets report_postmaster_not_running flag when postmaster death is detected or cannot be monitored
- Part of the kqueue-specific implementation of PostgreSQL's cross-platform event waiting infrastructure
- Optimizes for the common case where no changes are needed (early return when old_events == event->events)
- Essential for efficient I/O multiplexing on BSD-derived systems including macOS and FreeBSD

## Simplified Source

```c
// Simplified version of WaitEventAdjustKqueue
static void WaitEventAdjustKqueue(WaitEventSet *set, WaitEvent *event, int old_events) {
    struct kevent k_ev[2];
    int count = 0;
    bool old_filt_read = false, new_filt_read = false;
    bool old_filt_write = false, new_filt_write = false;

    // Early return if no changes needed
    if (old_events == event->events) {
        return;
    }

    // Handle different event types
    if (event->events == WL_POSTMASTER_DEATH) {
        // Monitor postmaster process death using process notification
        WaitEventAdjustKqueueAddPostmaster(&k_ev[count++], event);
    }
    else if (event->events == WL_LATCH_SET) {
        // Monitor latch wakeup using signal event
        WaitEventAdjustKqueueAddLatch(&k_ev[count++], event);
    }
    else {
        // Handle socket I/O events - compute changes for read/write filters

        // Determine old filter states
        if (old_events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED)) {
            old_filt_read = true;
        }
        if (old_events & WL_SOCKET_WRITEABLE) {
            old_filt_write = true;
        }

        // Determine new filter states
        if (event->events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED)) {
            new_filt_read = true;
        }
        if (event->events & WL_SOCKET_WRITEABLE) {
            new_filt_write = true;
        }

        // Add/remove read filter if changed
        if (old_filt_read && !new_filt_read) {
            WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_READ, EV_DELETE, event);
        } else if (!old_filt_read && new_filt_read) {
            WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_READ, EV_ADD, event);
        }

        // Add/remove write filter if changed
        if (old_filt_write && !new_filt_write) {
            WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_WRITE, EV_DELETE, event);
        } else if (!old_filt_write && new_filt_write) {
            WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_WRITE, EV_ADD, event);
        }
    }

    // Apply changes to kqueue if any events were prepared
    if (count > 0) {
        int rc = kevent(set->kqueue_fd, &k_ev[0], count, NULL, 0, NULL);

        // Handle errors, especially for postmaster death monitoring
        if (rc < 0) {
            if (event->events == WL_POSTMASTER_DEATH &&
                (errno == ESRCH || errno == EACCES)) {
                set->report_postmaster_not_running = true;
            } else {
                ereport(ERROR, (errcode_for_socket_access(),
                               errmsg("kevent() failed: %m")));
            }
        }
        // Check if postmaster actually died during registration
        else if (event->events == WL_POSTMASTER_DEATH &&
                 PostmasterPid != getppid() && !PostmasterIsAlive()) {
            set->report_postmaster_not_running = true;
        }
    }
}
```

Key simplifications made:
- Removed detailed assertions and validation logic
- Consolidated variable declarations at the top
- Added descriptive comments for each major section
- Simplified the filter state computation logic
- Streamlined the error handling while preserving essential checks
- Focused on the main execution path for clarity
- Maintained the core algorithm for managing kqueue events