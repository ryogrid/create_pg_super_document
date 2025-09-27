# WaitEventAdjustEpoll

## Location
[src/backend/storage/ipc/latch.c:1124-1175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L1124-L1175)

## Overview
Platform-specific function that manages epoll file descriptor registrations for wait events on Linux systems using the epoll mechanism.

## Definition
```c
static void WaitEventAdjustEpoll(WaitEventSet *set, WaitEvent *event, int action)
```

## Detailed Description
This internal function handles the low-level epoll system call operations for managing wait events on Linux systems. It translates PostgreSQL wait event types into corresponding epoll event flags and performs the requested epoll operation (add, modify, or delete).

The function builds an epoll_event structure based on the event type:
- **Latch events (WL_LATCH_SET)**: Registers for EPOLLIN to detect latch signals
- **Postmaster death events (WL_POSTMASTER_DEATH)**: Registers for EPOLLIN on the postmaster alive file descriptor
- **Socket events**: Maps WL_SOCKET_* flags to corresponding epoll flags:
  - WL_SOCKET_READABLE → EPOLLIN
  - WL_SOCKET_WRITEABLE → EPOLLOUT  
  - WL_SOCKET_CLOSED → EPOLLRDHUP

All events automatically include EPOLLERR and EPOLLHUP for error detection. The function uses the event pointer as epoll data to enable efficient event identification during epoll_wait().

## Parameters / Member Variables
- `set`: Pointer to the WaitEventSet containing the epoll file descriptor
- `event`: Pointer to the WaitEvent being adjusted
- `action`: epoll_ctl operation type (EPOLL_CTL_ADD, EPOLL_CTL_MOD, or EPOLL_CTL_DEL)

## Dependencies
- Functions called/Symbols referenced:
  - epoll_ctl (system call)
  - ereport (error reporting)
  - [errcode_for_socket_access](../e/errcode_for_socket_access.md)
- Called from (representative examples):
  - LatchWaitSetLatchPos
  - [AddWaitEventToSet](../A/AddWaitEventToSet.md)  
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md)

## Notes and Other Information
- Only compiled and available on Linux systems with WAIT_USE_EPOLL defined
- Always includes EPOLLERR and EPOLLHUP flags for comprehensive error detection
- Uses event pointer as epoll data for efficient callback identification
- Includes workaround for historical epoll bugs by always passing epoll_ev structure
- Validates that socket events have valid file descriptors and appropriate event flags
- Reports detailed error messages on epoll_ctl failures using PostgreSQL's error reporting system

## Simplified Source

```c
// Simplified version of WaitEventAdjustEpoll
static void WaitEventAdjustEpoll(WaitEventSet *set, WaitEvent *event, int action) {
    struct epoll_event epoll_ev;

    // Core logic step 1: Set up event data pointer for epoll callback
    epoll_ev.data.ptr = event;

    // Core logic step 2: Always monitor for errors
    epoll_ev.events = EPOLLERR | EPOLLHUP;

    // Core logic step 3: Configure event type-specific monitoring
    if (event->events == WL_LATCH_SET) {
        // Monitor latch signals
        epoll_ev.events |= EPOLLIN;
    } else if (event->events == WL_POSTMASTER_DEATH) {
        // Monitor postmaster death
        epoll_ev.events |= EPOLLIN;
    } else {
        // Monitor socket events
        if (event->events & WL_SOCKET_READABLE)
            epoll_ev.events |= EPOLLIN;
        if (event->events & WL_SOCKET_WRITEABLE)
            epoll_ev.events |= EPOLLOUT;
        if (event->events & WL_SOCKET_CLOSED)
            epoll_ev.events |= EPOLLRDHUP;
    }

    // Core logic step 4: Execute epoll operation
    int rc = epoll_ctl(set->epoll_fd, action, event->fd, &epoll_ev);

    // Core logic step 5: Handle errors
    if (rc < 0) {
        ereport(ERROR, (errcode_for_socket_access(),
                       errmsg("epoll_ctl() failed: %m")));
    }
}
```

Key simplifications made:
- Removed detailed assertions for clarity while preserving essential validation
- Consolidated error handling into a single check
- Added descriptive comments for each major logic step
- Simplified variable declarations by combining them where appropriate
- Focused on the main execution path without platform-specific workarounds