# GetWaitEventCustomNames

## Location
[src/backend/utils/activity/wait_event.c:307-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L307-L349)

## Overview
Returns a list of currently defined custom wait event names for a specified class, providing a way to enumerate all custom wait events.

## Definition
```c
char **GetWaitEventCustomNames(uint32 classId, int *nwaitevents)
```

## Detailed Description
This function retrieves all currently defined custom wait event names that belong to a specific class. It iterates through the WaitEventCustomHashByName hash table, filtering entries by the provided class ID, and returns an array of duplicated event name strings. The function allocates memory for the result array and ensures thread-safe access using shared locking. The caller receives both the array of names and the count of elements through an output parameter.

## Parameters / Member Variables
- `classId`: A 32-bit unsigned integer specifying the wait event class to filter by
- `nwaitevents`: Pointer to an integer where the number of wait events found will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [hash_get_num_entries](../h/hash_get_num_entries.md)
  - [palloc](../p/palloc.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [pstrdup](../p/pstrdup.md)
- Data structures used:
  - [WaitEventCustomEntryByName](../W/WaitEventCustomEntryByName.md)
  - WaitEventCustomHashByName
  - WaitEventCustomLock
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
- Constants used:
  - LW_SHARED
  - WAIT_EVENT_CLASS_MASK
- Called from (representative examples):
  - PG_GET_WAIT_EVENTS_COLS (in wait_event_funcs.c)

## Notes and Other Information
- Returns a palloc'd array that must be freed by the caller
- Uses shared locking to allow concurrent reads while preventing writes during enumeration
- Filters wait events by class using WAIT_EVENT_CLASS_MASK bitwise operations
- The returned array contains duplicated strings (pstrdup) for memory safety
- Function signature indicates it returns char** (array of string pointers)
- Located at src/backend/utils/activity/wait_event.c:307-349

## Simplified Source

```c
char **
GetWaitEventCustomNames(uint32 classId, int *nwaitevents)
{
    char **waiteventnames;
    WaitEventCustomEntryByName *hentry;
    HASH_SEQ_STATUS hash_seq;
    int index;
    int els;

    LWLockAcquire(WaitEventCustomLock, LW_SHARED);

    // Count total entries and allocate array
    els = hash_get_num_entries(WaitEventCustomHashByName);
    waiteventnames = palloc(els * sizeof(char *));

    // Scan hash table and collect names for matching class
    hash_seq_init(&hash_seq, WaitEventCustomHashByName);
    index = 0;

    while ((hentry = hash_seq_search(&hash_seq)) != NULL) {
        // Skip events not in our class
        if ((hentry->wait_event_info & WAIT_EVENT_CLASS_MASK) != classId)
            continue;

        // Copy event name to result array
        waiteventnames[index] = pstrdup(hentry->wait_event_name);
        index++;
    }

    LWLockRelease(WaitEventCustomLock);

    *nwaitevents = index;
    return waiteventnames;
}
```