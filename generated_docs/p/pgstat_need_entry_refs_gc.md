# pgstat_need_entry_refs_gc

## Location
[src/backend/utils/activity/pgstat_shmem.c:680-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L680-L695)

## Overview
A static helper function that determines whether garbage collection of statistics entry references is needed by comparing local and shared reference ages.

## Definition

```c
static bool
pgstat_need_entry_refs_gc(void)
```
## Detailed Description
This function checks if garbage collection of statistics entry references is required by comparing the local reference age () with the current garbage collection request count in shared memory. The function serves as a condition check to determine when cleanup operations should be performed.

The function first verifies that the entry reference hash table exists, then reads the current GC request count atomically from shared memory and compares it with the local reference age. If they differ, it indicates that garbage collection has been requested since the last cleanup, returning true to signal that GC is needed.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  - Atomically reads a 64-bit unsigned integer from shared memory
  -  - Local hash table for statistics entry references
  -  - Local variable tracking the reference age
  -  - Shared memory GC request counter

- Called from (representative examples):
  -  - [Hash](../H/Hash.md) table declaration macro that may reference this function
  -  - When getting entry references, checks if GC is needed

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Uses atomic read operations to safely access shared memory data
- Returns false if the entry reference hash table doesn't exist
- Contains an assertion to ensure  has been properly initialized
- The comparison between local age and shared counter provides an efficient way to detect when cleanup is needed
- Located in 