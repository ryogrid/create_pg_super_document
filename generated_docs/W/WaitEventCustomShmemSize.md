# WaitEventCustomShmemSize

## Location
[src/backend/utils/activity/wait_event.c:104-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L104-L119)

## Overview
Returns the shared memory size required for dynamic wait event custom hash tables and allocation counter data structures.

## Definition

```c
Size
WaitEventCustomShmemSize(void)
```
## Detailed Description
This function calculates the total shared memory space needed for the wait event custom subsystem. It computes the memory requirements for:
1. The custom wait event counter data structure (WaitEventCustomCounterData)
2. Two hash tables for custom wait events:
   - Hash table indexed by event info (WaitEventCustomEntryByInfo entries)
   - Hash table indexed by event name (WaitEventCustomEntryByName entries)

The function uses proper memory alignment (MAXALIGN) and safe size addition (add_size) to prevent overflow when calculating the total memory requirements.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN (memory alignment macro)
  - [add_size](../a/add_size.md) (safe size addition function)
  - [hash_estimate_size](../h/hash_estimate_size.md) (hash table size estimation function)
  - WaitEventCustomCounterData (counter data structure type)
  - WaitEventCustomEntryByInfo (hash entry type for info-based lookup)
  - WaitEventCustomEntryByName (hash entry type for name-based lookup)
  - WAIT_EVENT_CUSTOM_HASH_MAX_SIZE (maximum hash table size constant)

- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (in src/backend/storage/ipc/ipci.c:152)
  - PG_WAIT_INJECTIONPOINT (in src/include/utils/wait_event.h:62)

## Notes and Other Information
- This function is part of the shared memory initialization sequence and must be called before the actual shared memory allocation
- The calculated size includes space for both hash tables that will store custom wait event definitions
- Uses safe arithmetic functions to prevent integer overflow in size calculations
- The memory layout includes proper alignment considerations for optimal performance