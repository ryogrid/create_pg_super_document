# WaitLatchOrSocket

## Location
src/backend/storage/ipc/latch.c: 565 - 631

## Overview
WaitLatchOrSocket waits for one or more specified events to occur on a latch and optionally a socket, providing a unified interface for waiting on multiple types of events with a timeout.

## Definition


## Detailed Description
WaitLatchOrSocket is a wrapper around the WaitEventSet API that provides a convenient interface for waiting on a combination of latch signals and socket events. The function creates a temporary WaitEventSet, adds the requested events, waits for one of them to occur, and then cleans up the event set. For frequent usage scenarios, the documentation recommends creating a longer-living WaitEventSet directly for better efficiency.

The function supports waiting for latch signals, socket readiness (readable/writable/connected), postmaster death detection, and timeouts. When a socket condition is detected, EOF and error conditions are always reported as readable/writable/connected, allowing the caller to handle these special cases appropriately.

## Parameters / Member Variables
- : Pointer to the Latch object to monitor for signals
- : Bitmask specifying which events to wait for (WL_LATCH_SET, WL_SOCKET_*, WL_POSTMASTER_DEATH, WL_EXIT_ON_PM_DEATH, WL_TIMEOUT)
- : Socket descriptor to monitor for socket-related events (used when WL_SOCKET_* flags are set)
- : Maximum time to wait in milliseconds (-1 for infinite wait, >= 0 when WL_TIMEOUT is specified)
- : Information for wait event tracking and monitoring

## Dependencies
- Functions called/Symbols referenced:
  - CreateWaitEventSet
  - AddWaitEventToSet
  - WaitEventSetWait
  - FreeWaitEventSet
  - WL_TIMEOUT, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_EXIT_ON_PM_DEATH, WL_SOCKET_MASK constants
- Called from (representative examples):
  - read_or_wait (GSS API security)
  - be_tls_open_server (TLS/SSL operations)
  - libpqrcv_connect (WAL receiver connections)
  - LogicalRepApplyLoop (logical replication)

## Notes and Other Information
- This function is essentially a convenience wrapper around the more flexible WaitEventSet API
- Postmaster-managed processes must handle postmaster death by including either WL_EXIT_ON_PM_DEATH or WL_POSTMASTER_DEATH in wakeEvents
- For performance-critical code that waits frequently, consider using CreateWaitEventSet/WaitEventSetWait/FreeWaitEventSet directly
- Socket events include EOF and error conditions, which are reported as ready states to allow proper error handling
- Return value is a bitmask indicating which events actually occurred