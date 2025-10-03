# WaitLatchOrSocket

## Location
[src/backend/storage/ipc/latch.c:565-631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L565-L631)

## Overview
WaitLatchOrSocket waits for one or more specified events to occur on a latch and optionally a socket, providing a unified interface for waiting on multiple types of events with a timeout.

## Definition

```c
int
WaitLatchOrSocket(Latch *latch, int wakeEvents, pgsocket sock,
				  long timeout, uint32 wait_event_info)
```
## Detailed Description
WaitLatchOrSocket is a wrapper around the WaitEventSet API that provides a convenient interface for waiting on a combination of latch signals and socket events. The function creates a temporary WaitEventSet, adds the requested events, waits for one of them to occur, and then cleans up the event set. For frequent usage scenarios, the documentation recommends creating a longer-living WaitEventSet directly for better efficiency.

The function supports waiting for latch signals, socket readiness (readable/writable/connected), postmaster death detection, and timeouts. When a socket condition is detected, EOF and error conditions are always reported as readable/writable/connected, allowing the caller to handle these special cases appropriately.

## Parameters / Member Variables
- `*latch`: Pointer to the Latch object to monitor for signals
- `wakeEvents`: Bitmask specifying which events to wait for (WL_LATCH_SET, WL_SOCKET_*, WL_POSTMASTER_DEATH, WL_EXIT_ON_PM_DEATH, WL_TIMEOUT)
- `sock`: Socket descriptor to monitor for socket-related events (used when WL_SOCKET_* flags are set)
- `timeout`: Maximum time to wait in milliseconds (-1 for infinite wait, >= 0 when WL_TIMEOUT is specified)
- `wait_event_info`: Information for wait event tracking and monitoring
## Dependencies
- Functions called/Symbols referenced:
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)
  - [AddWaitEventToSet](../A/AddWaitEventToSet.md)
  - [WaitEventSetWait](WaitEventSetWait.md)
  - [FreeWaitEventSet](../F/FreeWaitEventSet.md)
  - WL_TIMEOUT, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_EXIT_ON_PM_DEATH, WL_SOCKET_MASK constants
- Called from (representative examples):
  - [read_or_wait](../r/read_or_wait.md) (GSS API security)
  - [be_tls_open_server](../b/be_tls_open_server.md) (TLS/SSL operations)
  - [libpqrcv_connect](../l/libpqrcv_connect.md) (WAL receiver connections)
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md) (logical replication)

## Notes and Other Information
- This function is essentially a convenience wrapper around the more flexible WaitEventSet API
- Postmaster-managed processes must handle postmaster death by including either WL_EXIT_ON_PM_DEATH or WL_POSTMASTER_DEATH in wakeEvents
- For performance-critical code that waits frequently, consider using CreateWaitEventSet/WaitEventSetWait/FreeWaitEventSet directly
- Socket events include EOF and error conditions, which are reported as ready states to allow proper error handling
- Return value is a bitmask indicating which events actually occurred

## Simplified Source

```c
// Simplified version of WaitLatchOrSocket
int WaitLatchOrSocket(Latch *latch, int wakeEvents, pgsocket sock,
                      long timeout, uint32 wait_event_info) {
    int ret = 0;
    int rc;
    WaitEvent event;

    // Create a temporary event set to hold our events
    WaitEventSet *set = CreateWaitEventSet(CurrentResourceOwner, 3);

    // Set timeout: use provided value if WL_TIMEOUT specified, otherwise infinite wait
    if (wakeEvents & WL_TIMEOUT)
        timeout = timeout;  // Use provided timeout
    else
        timeout = -1;       // Infinite wait

    // Add latch event if requested
    if (wakeEvents & WL_LATCH_SET)
        AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, latch, NULL);

    // Add postmaster death monitoring if requested
    if ((wakeEvents & WL_POSTMASTER_DEATH) && IsUnderPostmaster)
        AddWaitEventToSet(set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, NULL);

    if ((wakeEvents & WL_EXIT_ON_PM_DEATH) && IsUnderPostmaster)
        AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);

    // Add socket events if requested
    if (wakeEvents & WL_SOCKET_MASK) {
        int socket_events = wakeEvents & WL_SOCKET_MASK;
        AddWaitEventToSet(set, socket_events, sock, NULL, NULL);
    }

    // Wait for one of the events to occur
    rc = WaitEventSetWait(set, timeout, &event, 1, wait_event_info);

    // Process the result
    if (rc == 0) {
        ret |= WL_TIMEOUT;  // Timeout occurred
    } else {
        // Return which events actually fired
        ret |= event.events & (WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_MASK);
    }

    // Clean up the temporary event set
    FreeWaitEventSet(set);

    return ret;
}
```

Key simplifications made:
- Removed detailed assertions for clarity (kept the core logic)
- Added explanatory comments for each major step
- Simplified the timeout handling logic presentation
- Consolidated the event processing flow
- Focused on the main execution path: create event set → add events → wait → process result → cleanup
- Abstracted the low-level event masking details with descriptive comments