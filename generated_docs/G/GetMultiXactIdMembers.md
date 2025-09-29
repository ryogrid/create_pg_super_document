# GetMultiXactIdMembers

## Location
[src/backend/access/transam/multixact.c:1293-1580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1293-L1580)

## Overview
Retrieves the set of transaction members that compose a given MultiXactId, handling various edge cases and providing caching for performance optimization.

## Definition
```c
int GetMultiXactIdMembers(MultiXactId multi, MultiXactMember **members, bool from_pgupgrade, bool isLockOnly)
```

## Detailed Description
GetMultiXactIdMembers is a complex function that reads MultiXact member information from SLRU storage and returns the complete set of transactions that make up a given MultiXact. The function implements sophisticated logic to handle several corner cases that can occur in concurrent environments, particularly around MultiXact creation timing and offset wraparound scenarios.

The function first checks local cache for performance, then validates the MultiXact ID against known bounds. It handles special cases like pg_upgrade scenarios and lock-only MultiXacts. When reading from SLRU, it deals with complex concurrency issues where the next MultiXact might still be in the process of being created, using condition variables for coordination.

## Parameters / Member Variables
- `multi`: The MultiXactId whose members are to be retrieved
- `members`: Pointer to MultiXactMember array pointer that will be allocated and filled with member information (caller must free)
- `from_pgupgrade`: True if this MultiXact comes from a pg_upgrade scenario from 9.2 or older (returns -1 immediately)
- `isLockOnly`: True if the MultiXact is known to be used only for locking (allows optimization for old MultiXacts)

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid (validation)
  - [mXactCacheGetById](../m/mXactCacheGetById.md), mXactCachePut (caching operations)
  - [MultiXactIdSetOldestVisible](../M/MultiXactIdSetOldestVisible.md) (visibility management)
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md) (wraparound-aware comparison)
  - [LWLockAcquire](../L/LWLockAcquire.md), LWLockRelease (locking)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md), SimpleLruReadPage (SLRU operations)
  - [MultiXactIdToOffsetPage](../M/MultiXactIdToOffsetPage.md), MultiXactIdToOffsetEntry (page/entry calculation)
  - [MXOffsetToMemberPage](../M/MXOffsetToMemberPage.md), MXOffsetToMemberOffset (member location calculation)
  - [ConditionVariableSleep](../C/ConditionVariableSleep.md), ConditionVariableCancelSleep (concurrency coordination)
  - TransactionIdIsValid (transaction validation)
  - [palloc](../p/palloc.md) (memory allocation)
  - debug_elog3, debug_elog2 (debugging)
- Called from (representative examples):
  - [heap_lock_tuple](../h/heap_lock_tuple.md) (tuple locking operations)
  - [FreezeMultiXactId](../F/FreezeMultiXactId.md) (vacuum operations)
  - [MultiXactIdIsRunning](../M/MultiXactIdIsRunning.md) (visibility checking)
  - [DoesMultiXactIdConflict](../D/DoesMultiXactIdConflict.md) (conflict detection)

## Notes and Other Information
- Returns the number of members found, or -1 if none exist or invalid MultiXact
- Allocates memory for the members array using palloc - caller must free
- Implements extensive caching to avoid repeated SLRU reads
- Handles three complex corner cases around MultiXact creation timing
- Uses condition variables to coordinate with concurrent MultiXact creation
- Supports optimization for lock-only MultiXacts that can be quickly dismissed
- Handles pg_upgrade scenarios by returning empty results immediately
- Implements bank-based locking for efficient concurrent SLRU access
- Validates MultiXact bounds to detect wraparound conditions
- Stores results in local cache for future lookups

## Simplified Source

```c
int GetMultiXactIdMembers(MultiXactId multi, MultiXactMember **members,
                         bool from_pgupgrade, bool isLockOnly)
{
    // Handle special cases early
    if (!MultiXactIdIsValid(multi) || from_pgupgrade) {
        *members = NULL;
        return -1;
    }

    // Check cache first
    int length = mXactCacheGetById(multi, members);
    if (length >= 0)
        return length;

    // Set visibility for old MultiXact detection
    MultiXactIdSetOldestVisible();

    // Quick exit for old lock-only MultiXacts
    if (isLockOnly && MultiXactIdPrecedes(multi, OldestVisibleMXactId[MyProcNumber])) {
        *members = NULL;
        return -1;
    }

    // Validate MultiXact bounds
    LWLockAcquire(MultiXactGenLock, LW_SHARED);
    MultiXactId oldestMXact = MultiXactState->oldestMultiXactId;
    MultiXactId nextMXact = MultiXactState->nextMXact;
    MultiXactOffset nextOffset = MultiXactState->nextOffset;
    LWLockRelease(MultiXactGenLock);

    if (MultiXactIdPrecedes(multi, oldestMXact) || !MultiXactIdPrecedes(multi, nextMXact))
        ereport(ERROR, /* wraparound error */);

retry:
    // Read offset information for this MultiXact
    int64 pageno = MultiXactIdToOffsetPage(multi);
    int entryno = MultiXactIdToOffsetEntry(multi);

    LWLock *lock = SimpleLruGetBankLock(MultiXactOffsetCtl, pageno);
    LWLockAcquire(lock, LW_EXCLUSIVE);

    int slotno = SimpleLruReadPage(MultiXactOffsetCtl, pageno, true, multi);
    MultiXactOffset *offptr = (MultiXactOffset *) MultiXactOffsetCtl->shared->page_buffer[slotno];
    MultiXactOffset offset = offptr[entryno];

    // Calculate member count by finding next MultiXact's offset
    MultiXactId tmpMXact = multi + 1;
    if (nextMXact == tmpMXact) {
        // This is the latest MultiXact
        length = nextOffset - offset;
    } else {
        // Read next MultiXact's offset
        // Handle bank switching if needed...
        MultiXactOffset nextMXOffset = /* read next offset */;

        if (nextMXOffset == 0) {
            // Next MultiXact still being created - wait and retry
            LWLockRelease(lock);
            ConditionVariableSleep(&MultiXactState->nextoff_cv, WAIT_EVENT_MULTIXACT_CREATION);
            goto retry;
        }
        length = nextMXOffset - offset;
    }

    LWLockRelease(lock);

    // Allocate result array
    MultiXactMember *ptr = palloc(length * sizeof(MultiXactMember));

    // Read all member transaction IDs and flags
    int truelength = 0;
    for (int i = 0; i < length; i++, offset++) {
        // Read transaction ID and status from member pages
        TransactionId *xactptr = /* read from SLRU */;

        if (!TransactionIdIsValid(*xactptr))
            continue; // Skip invalid entries

        ptr[truelength].xid = *xactptr;
        ptr[truelength].status = /* read status flags */;
        truelength++;
    }

    // Cache the result and return
    mXactCachePut(multi, truelength, ptr);
    *members = ptr;
    return truelength;
}
```

This function:
1. Validates input and checks cache
2. Handles special cases (pg_upgrade, old lock-only MultiXacts)
3. Reads offset information to find member locations
4. Handles concurrency issues when next MultiXact is being created
5. Reads all member transaction IDs and status flags from SLRU
6. Caches results and returns the member array