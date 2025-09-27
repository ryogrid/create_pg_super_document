# WaitEventSetWaitBlock

## Location
[src/backend/storage/ipc/latch.c:1559-1692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L1559-L1692)

## Overview
WaitEventSetWaitBlock is the low-level blocking function that uses epoll on Linux to wait for I/O events and translates them into PostgreSQL's wait event format.

## Definition

```c
static inline int WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
                                        WaitEvent *occurred_events, int nevents)
```

## Detailed Description
WaitEventSetWaitBlock is a static inline function that implements the actual blocking wait operation using Linux's epoll_wait(2) system call. It serves as the platform-specific implementation layer beneath WaitEventSetWait, handling the translation between epoll events and PostgreSQL's unified wait event interface.

The function processes different types of events including latch notifications (through signalfd draining), postmaster death detection with paranoid validation, and socket I/O events (readable, writeable, closed). It carefully maps epoll event flags (EPOLLIN, EPOLLOUT, EPOLLHUP, etc.) to PostgreSQL's WL_* event constants.

For latch events, it drains the signalfd and validates the latch state. For postmaster death events, it performs additional validation by calling PostmasterIsAliveInternal() to prevent spurious death notifications. Socket events are mapped to readable, writeable, and closed states based on the corresponding epoll flags.

## Parameters / Member Variables
- `set`: WaitEventSet containing the epoll file descriptor and event configuration
- `cur_timeout`: Current timeout in milliseconds for this wait operation
- `occurred_events`: Output buffer to store translated wait events
- `nevents`: Maximum number of events to return

## Dependencies
- Functions called/Symbols referenced:
  - epoll_wait (system call)
  - [drain](../d/drain.md) (signalfd draining)
  - [PostmasterIsAliveInternal](../P/PostmasterIsAliveInternal.md)
  - [proc_exit](../p/proc_exit.md)
  - [errcode_for_socket_access](../e/errcode_for_socket_access.md)
  - ereport/errmsg (error reporting)
- Called from (representative examples):
  - [WaitEventSetWait](WaitEventSetWait.md)
  - LatchWaitSetLatchPos

## Notes and Other Information
- This is a Linux-specific implementation using epoll; other platforms have different implementations
- Returns -1 on timeout, 0 on interrupt/retry needed, or positive count of events processed
- Handles EINTR gracefully by returning 0 to retry the operation
- Performs paranoid validation for postmaster death events to prevent spurious notifications
- Maps epoll event masks to PostgreSQL's portable WL_* event constants
- Uses the epoll_event.data.ptr field to store pointers to corresponding WaitEvent structures for efficient event association

## Simplified Source

```c
// Simplified version of WaitEventSetWaitBlock
static inline int WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
                                        WaitEvent *occurred_events, int nevents) {
    int returned_events = 0;
    int rc;
    WaitEvent *cur_event;
    struct epoll_event *cur_epoll_event;

    // Core logic step 1: Wait for events using epoll
    rc = epoll_wait(set->epoll_fd, set->epoll_ret_events,
                    Min(nevents, set->nevents_space), cur_timeout);

    // Core logic step 2: Handle epoll_wait return status
    if (rc < 0) {
        // Handle interrupts gracefully, report other errors
        if (errno != EINTR) {
            waiting = false;
            ereport(ERROR, (errcode_for_socket_access(),
                           errmsg("epoll_wait() failed: %m")));
        }
        return 0;
    } else if (rc == 0) {
        // Timeout occurred
        return -1;
    }

    // Core logic step 3: Process each returned epoll event
    for (cur_epoll_event = set->epoll_ret_events;
         cur_epoll_event < (set->epoll_ret_events + rc) && returned_events < nevents;
         cur_epoll_event++) {

        cur_event = (WaitEvent *) cur_epoll_event->data.ptr;

        // Initialize output event structure
        occurred_events->pos = cur_event->pos;
        occurred_events->user_data = cur_event->user_data;
        occurred_events->events = 0;

        // Core logic step 4: Handle latch events
        if (cur_event->events == WL_LATCH_SET &&
            cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP)) {

            drain(); // Clear signalfd

            if (set->latch && set->latch->maybe_sleeping && set->latch->is_set) {
                occurred_events->fd = PGINVALID_SOCKET;
                occurred_events->events = WL_LATCH_SET;
                occurred_events++;
                returned_events++;
            }
        }
        // Core logic step 5: Handle postmaster death events
        else if (cur_event->events == WL_POSTMASTER_DEATH &&
                 cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP)) {

            // Verify postmaster is actually dead (paranoid check)
            if (!PostmasterIsAliveInternal()) {
                if (set->exit_on_postmaster_death)
                    proc_exit(1);
                occurred_events->fd = PGINVALID_SOCKET;
                occurred_events->events = WL_POSTMASTER_DEATH;
                occurred_events++;
                returned_events++;
            }
        }
        // Core logic step 6: Handle socket I/O events
        else if (cur_event->events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE | WL_SOCKET_CLOSED)) {

            // Map epoll events to PostgreSQL socket events
            if ((cur_event->events & WL_SOCKET_READABLE) &&
                (cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP))) {
                occurred_events->events |= WL_SOCKET_READABLE;
            }

            if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
                (cur_epoll_event->events & (EPOLLOUT | EPOLLERR | EPOLLHUP))) {
                occurred_events->events |= WL_SOCKET_WRITEABLE;
            }

            if ((cur_event->events & WL_SOCKET_CLOSED) &&
                (cur_epoll_event->events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP))) {
                occurred_events->events |= WL_SOCKET_CLOSED;
            }

            // Add to output if any events were set
            if (occurred_events->events != 0) {
                occurred_events->fd = cur_event->fd;
                occurred_events++;
                returned_events++;
            }
        }
    }

    return returned_events;
}
```

Key simplifications made:
- Preserved the essential epoll_wait logic and event processing flow
- Maintained critical error handling (EINTR, timeout, errors)
- Kept the three main event type handlers (latch, postmaster death, socket I/O)
- Simplified complex conditional logic while preserving functionality
- Added clear step-by-step comments for the main algorithm phases
- Focused on the core event translation mechanism from epoll to PostgreSQL events