# AddWaitEventToSet

## Location
[src/backend/storage/ipc/latch.c:963-1048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L963-L1048)

## Overview
Adds a wait event to a WaitEventSet, allowing the process to wait for various types of events including latch signals, socket I/O, and postmaster death.

## Definition
```c
int AddWaitEventToSet(WaitEventSet *set, uint32 events, pgsocket fd, Latch *latch, void *user_data)
```

## Detailed Description
This function adds a wait event to an existing WaitEventSet structure, enabling the process to wait for different types of asynchronous events. The function supports multiple event types:

- **WL_LATCH_SET**: Wait for a latch to be set (requires valid latch pointer)
- **WL_POSTMASTER_DEATH**: Wait for the postmaster process to die
- **WL_SOCKET_READABLE**: Wait for socket to become readable
- **WL_SOCKET_WRITEABLE**: Wait for socket to become writeable  
- **WL_SOCKET_CONNECTED**: Wait for socket connection establishment
- **WL_SOCKET_ACCEPT**: Wait for new connections on server socket
- **WL_SOCKET_CLOSED**: Wait for socket to be closed by remote peer
- **WL_EXIT_ON_PM_DEATH**: Combination flag that sets exit_on_postmaster_death

The function performs validation checks, assigns the event to the next available slot in the events array, and calls platform-specific adjustment functions (epoll, kqueue, poll, or Win32) to register the event with the underlying wait mechanism.

## Parameters / Member Variables
- `set`: Pointer to the WaitEventSet to add the event to
- `events`: Bitmask specifying which events to wait for (WL_* constants)
- `fd`: Socket file descriptor (use PGINVALID_SOCKET if not socket-related)
- `latch`: Pointer to latch structure (required for WL_LATCH_SET events)
- `user_data`: User-defined data pointer associated with this event

## Dependencies
- Functions called/Symbols referenced:
  - [WaitEventAdjustEpoll](../W/WaitEventAdjustEpoll.md) (Linux)
  - [WaitEventAdjustKqueue](../W/WaitEventAdjustKqueue.md) (BSD)
  - [WaitEventAdjustPoll](../W/WaitEventAdjustPoll.md) (poll-based systems)
  - [WaitEventAdjustWin32](../W/WaitEventAdjustWin32.md) (Windows)
  - elog (error reporting)
- Called from (representative examples):
  - [ExecAppendAsyncEventWait](../E/ExecAppendAsyncEventWait.md)
  - [pq_init](../p/pq_init.md)
  - [ConfigurePostmasterWaitSet](../C/ConfigurePostmasterWaitSet.md)
  - [SysLoggerMain](../S/SysLoggerMain.md)
  - [InitializeLatchWaitSet](../I/InitializeLatchWaitSet.md)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md)

## Notes and Other Information
- Returns the position index in the events array, which can be used with ModifyWaitEvent()
- Validates that latches are owned by the current process and only one latch per set
- Performs platform-specific initialization through conditional compilation
- Socket events require a valid file descriptor
- The WL_EXIT_ON_PM_DEATH flag is internally converted to WL_POSTMASTER_DEATH with an additional exit flag set
- On Windows, includes additional reset field initialization

## Simplified Source

```c
// Simplified version of AddWaitEventToSet
int AddWaitEventToSet(WaitEventSet *set, uint32 events, pgsocket fd, Latch *latch, void *user_data) {
    WaitEvent *event;

    // Ensure we have space for another event
    Assert(set->nevents < set->nevents_space);

    // Handle special exit-on-postmaster-death case
    if (events == WL_EXIT_ON_PM_DEATH) {
        events = WL_POSTMASTER_DEATH;
        set->exit_on_postmaster_death = true;
    }

    // Validate latch usage
    if (latch) {
        // Must own the latch and only one latch per set allowed
        if (latch->owner_pid != MyProcPid)
            elog(ERROR, "cannot wait on a latch owned by another process");
        if (set->latch)
            elog(ERROR, "cannot wait on more than one latch");
        if ((events & WL_LATCH_SET) != WL_LATCH_SET)
            elog(ERROR, "latch events only support being set");
    } else if (events & WL_LATCH_SET) {
        elog(ERROR, "cannot wait on latch without a specified latch");
    }

    // Validate socket usage
    if (fd == PGINVALID_SOCKET && (events & WL_SOCKET_MASK))
        elog(ERROR, "cannot wait on socket event without a socket");

    // Initialize the new event
    event = &set->events[set->nevents];
    event->pos = set->nevents++;
    event->fd = fd;
    event->events = events;
    event->user_data = user_data;

    // Handle special event types
    if (events == WL_LATCH_SET) {
        set->latch = latch;
        set->latch_pos = event->pos;
        // Set platform-specific file descriptor for latch monitoring
        #if defined(WAIT_USE_SELF_PIPE)
            event->fd = selfpipe_readfd;
        #elif defined(WAIT_USE_SIGNALFD)
            event->fd = signal_fd;
        #else
            event->fd = PGINVALID_SOCKET;
        #endif
    } else if (events == WL_POSTMASTER_DEATH) {
        // Set file descriptor for postmaster monitoring (Unix only)
        #ifndef WIN32
            event->fd = postmaster_alive_fds[POSTMASTER_FD_WATCH];
        #endif
    }

    // Register event with platform-specific wait mechanism
    #if defined(WAIT_USE_EPOLL)
        WaitEventAdjustEpoll(set, event, EPOLL_CTL_ADD);
    #elif defined(WAIT_USE_KQUEUE)
        WaitEventAdjustKqueue(set, event, 0);
    #elif defined(WAIT_USE_POLL)
        WaitEventAdjustPoll(set, event);
    #elif defined(WAIT_USE_WIN32)
        WaitEventAdjustWin32(set, event);
    #endif

    return event->pos;
}
```

Key simplifications made:
- Condensed validation logic with clearer comments explaining each check
- Grouped related validation checks together (latch validation, socket validation)
- Simplified the conditional compilation blocks with clear comments about their purpose
- Removed Windows-specific reset field initialization detail from main flow
- Preserved all essential error checking and platform-specific behavior
- Maintained the core algorithm while making the flow more readable