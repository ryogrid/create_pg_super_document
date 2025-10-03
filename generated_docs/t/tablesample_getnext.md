# tablesample_getnext

## Location
[src/backend/executor/nodeSamplescan.c:320-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSamplescan.c#L320-L363)

## Overview
Retrieves the next tuple from a TABLESAMPLE scan by coordinating block-level and tuple-level sampling operations.

## Definition
static TupleTableSlot *tablesample_getnext(SampleScanState *scanstate)

## Detailed Description
This static function implements the core tuple retrieval logic for table sampling scans. It operates in a two-level approach: first ensuring a valid block is available for scanning, then retrieving individual tuples from that block. The function manages the scan state by tracking whether a block is currently available (haveblock) and whether the scan is complete (done). When a block is exhausted of tuples, it automatically advances to the next block. The function maintains a count of returned tuples and ensures only visible tuples are returned to the caller.

## Parameters / Member Variables
- `scanstate`: Pointer to the SampleScanState containing the current scan state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - [SampleScanState](../S/SampleScanState.md) (scan state structure)
  - [TableScanDesc](../T/TableScanDesc.md) (table scan descriptor)
  - [ExecClearTuple](../E/ExecClearTuple.md) (tuple slot management)
  - [table_scan_sample_next_block](table_scan_sample_next_block.md) (block-level sampling)
  - [table_scan_sample_next_tuple](table_scan_sample_next_tuple.md) (tuple-level sampling)
- Called from (representative examples):
  - [SampleNext](../S/SampleNext.md) (in nodeSamplescan.c:53)

## Notes and Other Information
The function implements a state machine with the following logic:
- Returns NULL immediately if the scan is marked as done
- Enters an infinite loop to handle block transitions:
  - If no current block, attempts to get the next sample block
  - If no more blocks available, marks scan as done and returns NULL
  - Attempts to get the next tuple from the current block
  - If block is exhausted, marks haveblock as false and continues to next block
  - If a visible tuple is found, breaks the loop and returns it

The function increments the donetuples counter for each successfully returned tuple, providing a running count of tuples processed during the scan. This counter is useful for sampling methods that need to track progress or implement tuple-count-based sampling strategies.

## Simplified Source

```c
static TupleTableSlot *
tablesample_getnext(SampleScanState *scanstate)
{
    TableScanDesc scan = scanstate->ss.ss_currentScanDesc;
    TupleTableSlot *slot = scanstate->ss.ss_ScanTupleSlot;

    ExecClearTuple(slot);

    // Return NULL if scan is already completed
    if (scanstate->done)
        return NULL;

    // Main sampling loop
    for (;;)
    {
        // Get next block if we don't have one
        if (!scanstate->haveblock)
        {
            if (!table_scan_sample_next_block(scan, scanstate))
            {
                // No more blocks - scan complete
                scanstate->done = true;
                return NULL;
            }
            scanstate->haveblock = true;
        }

        // Try to get next tuple from current block
        if (!table_scan_sample_next_tuple(scan, scanstate, slot))
        {
            // Block exhausted, move to next block
            scanstate->haveblock = false;
            continue;
        }

        // Found a visible tuple
        break;
    }

    scanstate->donetuples++;
    return slot;
}
```