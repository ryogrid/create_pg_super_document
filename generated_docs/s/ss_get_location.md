# ss_get_location

## Location
[src/backend/access/common/syncscan.c:254-288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/syncscan.c#L254-L288)

## Overview
Retrieves the optimal starting location for a sequential scan based on previously recorded scan positions from other concurrent scans.

## Definition
```c
BlockNumber ss_get_location(Relation rel, BlockNumber relnblocks)
```

## Detailed Description
ss_get_location is a public function that provides the entry point for PostgreSQL's synchronized scan feature. When a new sequential scan is started, this function determines the best starting position to coordinate with other concurrent scans of the same table.

The function works by:
1. **Lock Acquisition**: Acquires an exclusive lock on SyncScanLock to ensure thread-safe access to shared scan location data
2. **Location Lookup**: Calls ss_search with the relation's locator to find any existing scan position
3. **Validation**: Checks if the returned location is still valid (less than current table size)
4. **Fallback**: Returns 0 if no valid location is found or if the stored location is beyond the current table size

The goal is to have new scans start close to where existing scans are currently positioned, reducing overall I/O by allowing buffer cache sharing between concurrent scans.

## Parameters / Member Variables
- `rel`: Pointer to the Relation structure representing the table being scanned
- `relnblocks`: The current number of blocks in the relation (obtained via RelationGetNumberOfBlocks)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [ss_search](ss_search.md) (internal function)
  - SyncScanLock (lock identifier)
- Called from (representative examples):
  - [initscan](../i/initscan.md)
  - [table_block_parallelscan_startblock_init](../t/table_block_parallelscan_startblock_init.md)

## Notes and Other Information
- Returns 0 if no synchronized scan location is available or if the stored location is invalid
- The function ensures the returned location is always less than relnblocks (when relnblocks > 0)
- Handles cases where the table has been truncated since the location was last updated
- Uses exclusive locking to prevent race conditions when accessing shared scan location data
- Includes optional trace logging when TRACE_SYNCSCAN is enabled for debugging purposes
- Critical for the synchronized scan optimization that reduces redundant I/O in concurrent table scans

## Simplified Source

```c
BlockNumber ss_get_location(Relation rel, BlockNumber relnblocks)
{
    BlockNumber startloc;

    // Get the last reported scan location with exclusive lock
    LWLockAcquire(SyncScanLock, LW_EXCLUSIVE);
    startloc = ss_search(rel->rd_locator, 0, false);
    LWLockRelease(SyncScanLock);

    // Validate location is within current table size
    // (table may have been truncated since location was saved)
    if (startloc >= relnblocks)
        startloc = 0;

#ifdef TRACE_SYNCSCAN
    if (trace_syncscan)
        elog(LOG, "SYNC_SCAN: start \"%s\" (size %u) at %u",
             RelationGetRelationName(rel), relnblocks, startloc);
#endif

    return startloc;
}
```