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
  - InitializeLatchWaitSet
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md)

## Notes and Other Information
- Returns the position index in the events array, which can be used with ModifyWaitEvent()
- Validates that latches are owned by the current process and only one latch per set
- Performs platform-specific initialization through conditional compilation
- Socket events require a valid file descriptor
- The WL_EXIT_ON_PM_DEATH flag is internally converted to WL_POSTMASTER_DEATH with an additional exit flag set
- On Windows, includes additional reset field initialization