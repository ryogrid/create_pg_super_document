# WaitEventSetWait

## Location
[src/backend/storage/ipc/latch.c:1424-1567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L1424-L1567)

## Overview
WaitEventSetWait is the main function for waiting on multiple events in PostgreSQL's event-driven I/O system, providing timeout support and returning information about occurred events.

## Definition

```c
int
WaitEventSetWait(WaitEventSet *set, long timeout,
				 WaitEvent *occurred_events, int nevents,
				 uint32 wait_event_info)
```
## Detailed Description
WaitEventSetWait implements the core waiting logic for PostgreSQL's event system. It waits for events registered in a WaitEventSet to occur, with sophisticated timeout handling and latch management. The function handles both blocking and non-blocking scenarios, manages signal processing on Windows, and provides precise timeout calculations for partial waits.

The function first checks if a latch is already set, and if so, reports it immediately. It then uses WaitEventSetWaitBlock to wait for actual I/O events. The function includes memory barriers for proper synchronization on weak memory ordering machines and handles the 'maybe_sleeping' flag to optimize latch notifications.

For timeout management, it records the start time and recalculates remaining timeout after interruptions, ensuring accurate timeout behavior even when the wait is interrupted and resumed.

## Parameters / Member Variables
- `set`: WaitEventSet containing the events to wait for
- `timeout`: Maximum time to wait in milliseconds (-1 for infinite, 0 for non-blocking, >0 for specific timeout)
- `occurred_events`: Output buffer to store information about events that occurred
- `nevents`: Maximum number of events to return (size of occurred_events buffer)
- `wait_event_info`: Wait event information for statistics reporting

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_SET_CURRENT
  - INSTR_TIME_SET_ZERO
  - INSTR_TIME_SUBTRACT
  - INSTR_TIME_GET_MILLISEC
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - [pgwin32_dispatch_queued_signals](../p/pgwin32_dispatch_queued_signals.md) (Windows only)
  - [WaitEventSetWaitBlock](WaitEventSetWaitBlock.md)
  - pg_memory_barrier
- Called from (representative examples):
  - [WaitLatch](WaitLatch.md)
  - [WaitLatchOrSocket](WaitLatchOrSocket.md)
  - [ServerLoop](../S/ServerLoop.md)
  - [WalSndWait](WalSndWait.md)
  - [secure_read](../s/secure_read.md)/secure_write

## Notes and Other Information
- Returns the number of events that occurred, or 0 if timeout was reached
- Sets the maybe_sleeping flag on latches to optimize notification delivery
- Handles signal processing differently on Windows vs Unix platforms
- Uses memory barriers to ensure proper synchronization on weak memory ordering architectures
- Supports partial event collection when the latch is set but additional events are available
- The function is central to PostgreSQL's event-driven architecture and is used throughout the system for I/O multiplexing

## Simplified Source

```c
// Simplified version of WaitEventSetWait
int WaitEventSetWait(WaitEventSet *set, long timeout,
                     WaitEvent *occurred_events, int nevents,
                     uint32 wait_event_info) {
    int returned_events = 0;
    instr_time start_time, cur_time;
    long cur_timeout = -1;

    // Initialize timeout tracking if needed
    if (timeout >= 0) {
        record_start_time(&start_time);
        cur_timeout = timeout;
    }

    // Report wait start for statistics
    pgstat_report_wait_start(wait_event_info);

    // Platform-specific signal handling setup
    setup_signal_handling();

    // Main event waiting loop
    while (returned_events == 0) {
        int rc;

        // Check if latch is already set
        if (set->latch && !set->latch->is_set) {
            // About to sleep - set flag for optimization
            set->latch->maybe_sleeping = true;
            memory_barrier();
        }

        // Handle latch events immediately if set
        if (set->latch && set->latch->is_set) {
            // Record latch event
            occurred_events->fd = PGINVALID_SOCKET;
            occurred_events->pos = set->latch_pos;
            occurred_events->user_data = set->events[set->latch_pos].user_data;
            occurred_events->events = WL_LATCH_SET;
            occurred_events++;
            returned_events++;

            set->latch->maybe_sleeping = false;

            // Exit if buffer full, otherwise poll for more events with zero timeout
            if (returned_events == nevents)
                break;
            cur_timeout = 0;
            timeout = 0;
        }

        // Wait for events using platform-specific mechanism
        rc = WaitEventSetWaitBlock(set, cur_timeout,
                                  occurred_events, nevents - returned_events);

        // Clean up latch sleeping flag
        if (set->latch && set->latch->maybe_sleeping)
            set->latch->maybe_sleeping = false;

        // Handle wait results
        if (rc == -1)
            break;  // Timeout occurred
        else
            returned_events += rc;

        // Update remaining timeout for next iteration
        if (returned_events == 0 && timeout >= 0) {
            calculate_remaining_timeout(&start_time, &cur_time, timeout, &cur_timeout);
            if (cur_timeout <= 0)
                break;
        }
    }

    // Platform-specific cleanup
    cleanup_signal_handling();

    // Report wait end for statistics
    pgstat_report_wait_end();

    return returned_events;
}
```

Key simplifications made:
- Abstracted platform-specific signal handling into helper functions
- Simplified timeout calculation logic into a helper function
- Removed detailed comments about memory ordering and pipe behavior
- Consolidated similar conditional blocks
- Focused on the main execution flow while preserving core algorithm
- Abstracted low-level time measurement operations