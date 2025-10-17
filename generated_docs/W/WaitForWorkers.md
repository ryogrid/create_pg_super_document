# WaitForWorkers

## Location
[src/bin/pg_dump/parallel.c:1451-1515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1451-L1515)

## Overview
Coordinates worker process completion in pg_dump's parallel processing system by implementing flexible waiting strategies for different synchronization requirements.

## Definition

```c
void
WaitForWorkers(ArchiveHandle *AH, ParallelState *pstate, WFW_WaitOption mode)
```
## Detailed Description
This function provides the main coordination mechanism for the leader process to synchronize with worker processes in pg_dump's parallel architecture. It implements four distinct waiting strategies:

- **WFW_NO_WAIT**: Non-blocking mode that reaps any available status messages but returns immediately
- **WFW_GOT_STATUS**: Blocks until at least one worker completes and sends a status message
- **WFW_ONE_IDLE**: Waits until at least one worker becomes available for new work assignments
- **WFW_ALL_IDLE**: Blocks until all workers have completed their current tasks and are idle

The function operates in a loop that calls ListenToWorkers to collect status messages and then evaluates the current worker state against the requested waiting condition. It uses dynamic blocking behavior - starting non-blocking (except for GOT_STATUS mode) and switching to blocking mode if the termination condition isn't met on the first iteration.

This design enables efficient resource utilization by allowing the leader to dispatch new work as soon as workers become available while also supporting batch synchronization when needed.

## Parameters / Member Variables
- `*AH`: Archive handle containing database connection and operation context for passing to worker status handlers
- `*pstate`: Parallel state structure containing worker slots, job assignments, and worker status tracking
- `mode`: Enumerated wait strategy defining the termination condition (WFW_NO_WAIT, WFW_GOT_STATUS, WFW_ONE_IDLE, WFW_ALL_IDLE)
## Dependencies
- Functions called/Symbols referenced:
  - [ListenToWorkers](../L/ListenToWorkers.md) (collects and processes worker status messages)
  - [IsEveryWorkerIdle](../I/IsEveryWorkerIdle.md) (checks if all workers are in idle state)
  - [GetIdleWorker](../G/GetIdleWorker.md) (finds an available worker slot)
  - WFW_WaitOption (enumeration defining wait modes)
  - [ParallelState](../P/ParallelState.md) (parallel processing state structure)
  - NO_SLOT (constant indicating no worker slot available)

- Called from (representative examples):
  - [DispatchJobForTocEntry](../D/DispatchJobForTocEntry.md) (job dispatch and worker coordination)
  - [WriteDataChunks](WriteDataChunks.md) (parallel data writing operations)
  - [restore_toc_entries_parallel](../r/restore_toc_entries_parallel.md) (parallel restore coordination)

## Notes and Other Information
- Executed exclusively in the leader process for coordinating worker activities
- Supports both eager and lazy synchronization patterns depending on mode
- Uses assertions to validate proper usage (e.g., GOT_STATUS mode requires active workers)
- Central to pg_dump's load balancing by enabling dynamic work redistribution
- Optimized for common case where typically only one worker completes at a time
- Essential for preventing resource exhaustion and ensuring orderly shutdown of parallel operations

## Simplified Source

```c
void WaitForWorkers(ArchiveHandle *AH, ParallelState *pstate, WFW_WaitOption mode) {
    bool do_wait = false;

    // In GOT_STATUS mode, always wait for a message initially
    if (mode == WFW_GOT_STATUS) {
        Assert(!IsEveryWorkerIdle(pstate));  // Must have active workers
        do_wait = true;
    }

    for (;;) {
        // Check for status messages from workers
        if (ListenToWorkers(AH, pstate, do_wait)) {
            // Got a message - check if we're done based on mode
            if (mode != WFW_ALL_IDLE) {
                return;  // Done for all modes except ALL_IDLE
            }
        }

        // Check termination conditions based on wait mode
        switch (mode) {
            case WFW_NO_WAIT:
                return;  // Never wait, just collect available messages

            case WFW_GOT_STATUS:
                Assert(false);  // Should have returned above after getting message

            case WFW_ONE_IDLE:
                if (GetIdleWorker(pstate) != NO_SLOT) {
                    return;  // Found at least one idle worker
                }
                break;

            case WFW_ALL_IDLE:
                if (IsEveryWorkerIdle(pstate)) {
                    return;  // All workers are now idle
                }
                break;
        }

        // If we get here, wait for something to happen
        do_wait = true;
    }
}
```