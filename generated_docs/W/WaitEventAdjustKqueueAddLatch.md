# WaitEventAdjustKqueueAddLatch

## Location
src/backend/storage/ipc/latch.c: 1248 - 1262

## Overview
A static inline function that configures a kevent structure to monitor latch signaling events using the SIGURG signal through the kqueue EVFILT_SIGNAL filter.

## Definition
```c
static inline void
WaitEventAdjustKqueueAddLatch(struct kevent *k_ev, WaitEvent *event)
```

## Detailed Description
This function sets up kqueue-based monitoring for latch signaling in PostgreSQL's event waiting system. It configures a kevent structure to watch for SIGURG signals using the EVFILT_SIGNAL filter. Latches in PostgreSQL are lightweight synchronization primitives used for inter-process communication, and this function enables kqueue-based systems to efficiently wait for latch events. The function currently only supports adding latch monitoring and does not provide removal functionality.

## Parameters / Member Variables
- `k_ev`: Pointer to the kevent structure to be configured for latch signal monitoring
- `event`: Pointer to the WaitEvent structure that will be associated with this kevent

## Dependencies
- Functions called/Symbols referenced:
  - SIGURG (signal number used for latch signaling)
  - AccessWaitEvent (macro for associating WaitEvent with kevent)
  - [WaitEvent](WaitEvent.md) (structure type)
  - EVFILT_SIGNAL (kqueue filter for signal events)
  - EV_ADD (kqueue flag to add the event)
- Called from (representative examples):
  - [WaitEventAdjustKqueue](WaitEventAdjustKqueue.md) (at line 1295)

## Notes and Other Information
- This function only supports adding latch monitoring, not removing it, as indicated by the comment
- Uses SIGURG (urgent signal) as the mechanism for latch signaling on kqueue systems
- Part of PostgreSQL's cross-platform latch implementation that provides efficient inter-process signaling
- The EVFILT_SIGNAL filter allows kqueue to monitor for specific signal deliveries
- Essential component of PostgreSQL's event-driven architecture for background processes and inter-process communication
- Provides an alternative to polling-based approaches for latch waiting on BSD-derived systems