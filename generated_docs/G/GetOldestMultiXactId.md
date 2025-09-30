# GetOldestMultiXactId

## Location
[src/backend/access/transam/multixact.c:2652-2704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2652-L2704)

## Overview
Returns the oldest MultiXactId that could still be considered live by any running transaction, used for determining safe points for vacuum operations and SLRU management.

## Definition
MultiXactId GetOldestMultiXactId(void)

## Detailed Description
This function determines the oldest MultiXactId that might still be referenced by any active transaction in the system. It examines all entries in the OldestMemberMXactId and OldestVisibleMXactId arrays to find the minimum valid value across all slots. If no valid entries exist, it returns the next MultiXactId to be assigned.

The function is critical for vacuum operations and SLRU management decisions. While it's not safe to truncate MultiXact SLRU segments based solely on this value, it can be used to set relminmxid for tables that VACUUM knows have no remaining MXIDs older than this value. The function handles wraparound conditions carefully by ensuring that nextMXact is normalized to a valid range.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (MultiXactGenLock, LW_SHARED)
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - FirstMultiXactId
  - MaxOldestSlot
  - OldestMemberMXactId array
  - OldestVisibleMXactId array
- Called from (representative examples):
  - [heapam_relation_set_new_filelocator](../h/heapam_relation_set_new_filelocator.md)
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md)
  - [vac_update_datfrozenxid](../v/vac_update_datfrozenxid.md)

## Notes and Other Information
- Uses shared locking on MultiXactGenLock for safe concurrent access
- Handles wraparound conditions by normalizing nextMXact to valid range
- Examines both OldestMemberMXactId and OldestVisibleMXactId arrays
- Critical for VACUUM operations and determining safe truncation points
- Does not guarantee that returned value is safe for SLRU truncation
- Safe for setting relminmxid values in vacuum operations
- Returns the most conservative (oldest) valid MultiXactId found

## Simplified Source

```c
MultiXactId
GetOldestMultiXactId(void)
{
    MultiXactId oldestMXact;
    MultiXactId nextMXact;
    int i;

    // Acquire shared lock to read MultiXact state
    LWLockAcquire(MultiXactGenLock, LW_SHARED);

    // Get next MultiXact ID, handling wraparound
    nextMXact = MultiXactState->nextMXact;
    if (nextMXact < FirstMultiXactId)
        nextMXact = FirstMultiXactId;

    // Start with next MXID as default oldest
    oldestMXact = nextMXact;

    // Scan all slots to find the oldest valid MultiXactId
    for (i = 0; i < MaxOldestSlot; i++)
    {
        MultiXactId thisoldest;

        // Check oldest member MXID for this slot
        thisoldest = OldestMemberMXactId[i];
        if (MultiXactIdIsValid(thisoldest) &&
            MultiXactIdPrecedes(thisoldest, oldestMXact))
            oldestMXact = thisoldest;

        // Check oldest visible MXID for this slot
        thisoldest = OldestVisibleMXactId[i];
        if (MultiXactIdIsValid(thisoldest) &&
            MultiXactIdPrecedes(thisoldest, oldestMXact))
            oldestMXact = thisoldest;
    }

    LWLockRelease(MultiXactGenLock);

    return oldestMXact;
}
```