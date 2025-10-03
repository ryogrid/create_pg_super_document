# gather_readnext

## Location
[src/backend/executor/nodeGather.c:304-392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGather.c#L304-L392)

## Overview
Implements the worker tuple reading logic for Gather nodes, managing round-robin reading from multiple worker process tuple queues with non-blocking I/O and worker lifecycle management.

## Definition

```c
static MinimalTuple
gather_readnext(GatherState *gatherstate)
```
## Detailed Description
gather_readnext manages the complex task of reading tuples from multiple parallel worker processes in a fair and efficient manner. It implements a round-robin strategy that reads from worker tuple queues, handling worker failures gracefully and managing the dynamic removal of completed workers from the active reader array. The function uses non-blocking reads to avoid getting stuck on slow workers, and when no tuples are immediately available from any worker, it either returns NULL (to allow local execution) or blocks using WaitLatch.

The function maintains an array of active TupleQueueReader objects and tracks the current position with nextreader. When a worker completes, it's removed from the array using memmove, and the reader count is adjusted. The function includes an optimization where it continues reading from the same worker until that would require blocking, rather than advancing after every tuple, which proves more efficient in practice.

## Parameters / Member Variables
- `*gatherstate`: The GatherState containing the array of active tuple queue readers and worker management state
## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (allows query cancellation during worker communication)
  - [TupleQueueReaderNext](../T/TupleQueueReaderNext.md) (reads next tuple from specific worker, non-blocking)
  - [ExecShutdownGatherWorkers](../E/ExecShutdownGatherWorkers.md) (shuts down workers when all are complete)
  - memmove (compacts reader array when workers complete)
  - [WaitLatch](../W/WaitLatch.md) (blocks waiting for worker activity or system events)
  - [ResetLatch](../R/ResetLatch.md) (clears latch after waking up)
- Called from (representative examples):
  - [gather_getnext](gather_getnext.md) (main tuple retrieval function)

## Notes and Other Information
- Uses round-robin scheduling but with an efficiency optimization: continues reading from same worker until blocking would be required
- Handles worker failures gracefully by treating failed workers as having produced no tuples
- Dynamically manages the reader array, compacting it when workers complete
- Non-blocking reads prevent the system from getting stuck on slow workers
- When all immediate sources are exhausted but local scanning is enabled, returns NULL to allow local execution
- Uses WaitLatch with WL_LATCH_SET and WL_EXIT_ON_PM_DEATH for proper signal handling
- The nvisited counter ensures all workers are checked before blocking
- Worker cleanup is triggered automatically when the last worker completes
- Critical for maintaining fairness and preventing starvation in parallel execution
- Integrates with PostgreSQL's latch-based inter-process communication system

## Simplified Source

```c
static MinimalTuple
gather_readnext(GatherState *gatherstate)
{
    int nvisited = 0;

    for (;;) {
        TupleQueueReader *reader;
        MinimalTuple tup;
        bool readerdone;

        CHECK_FOR_INTERRUPTS();

        // Try to read from current worker (non-blocking)
        Assert(gatherstate->nextreader < gatherstate->nreaders);
        reader = gatherstate->reader[gatherstate->nextreader];
        tup = TupleQueueReaderNext(reader, true, &readerdone);

        // Handle completed worker
        if (readerdone) {
            Assert(!tup);
            --gatherstate->nreaders;

            if (gatherstate->nreaders == 0) {
                // All workers done
                ExecShutdownGatherWorkers(gatherstate);
                return NULL;
            }

            // Remove completed worker from array
            memmove(&gatherstate->reader[gatherstate->nextreader],
                   &gatherstate->reader[gatherstate->nextreader + 1],
                   sizeof(TupleQueueReader *) *
                   (gatherstate->nreaders - gatherstate->nextreader));

            if (gatherstate->nextreader >= gatherstate->nreaders)
                gatherstate->nextreader = 0;
            continue;
        }

        // Return tuple if we got one
        if (tup)
            return tup;

        // Advance to next reader in round-robin fashion
        gatherstate->nextreader++;
        if (gatherstate->nextreader >= gatherstate->nreaders)
            gatherstate->nextreader = 0;

        // Check if we've visited all workers
        nvisited++;
        if (nvisited >= gatherstate->nreaders) {
            // Allow local execution if needed
            if (gatherstate->need_to_scan_locally)
                return NULL;

            // Wait for worker activity
            (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
                           WAIT_EVENT_EXECUTE_GATHER);
            ResetLatch(MyLatch);
            nvisited = 0;
        }
    }
}
```