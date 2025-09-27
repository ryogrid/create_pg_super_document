# find_active_timeout

## Location
[src/backend/utils/misc/timeout.c:96-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L96-L113)

## Overview
Searches the active timeout array to find the index of a timeout with the specified ID.

## Definition

```c
static int
find_active_timeout(TimeoutId id)
```
## Detailed Description
This internal helper function searches through the  array to locate a timeout entry with the given . The function performs a linear search through all active timeouts and returns the array index of the matching timeout entry. If no matching timeout is found, the function returns -1.

The function is designed to be called from within the timeout management subsystem when the caller needs to locate a specific timeout entry for operations like enabling, disabling, or modifying timeout properties.

## Parameters / Member Variables
- **id**: The TimeoutId to search for in the active timeouts array

## Dependencies
- Functions called/Symbols referenced:
  - [TimeoutId](../T/TimeoutId.md) (data type)
- Called from (representative examples):
  - [enable_timeout](../e/enable_timeout.md)
  - [disable_timeout](../d/disable_timeout.md)
  - [disable_timeouts](../d/disable_timeouts.md)

## Notes and Other Information
- This is a static function internal to the timeout.c module
- The function performs a linear search, so performance scales with the number of active timeouts
- It is the caller's responsibility to protect this function from signal handler interruption
- Typically called after disable_alarm() and before schedule_alarm() in timeout management operations
- Returns -1 when the timeout ID is not found in the active array
- Part of the internal helper functions for PostgreSQL's timeout management system

## Simplified Source

```c
// Simplified version of find_active_timeout
static int find_active_timeout(TimeoutId id) {
    // Search through all active timeouts
    for (int i = 0; i < num_active_timeouts; i++) {
        // Check if this timeout matches the requested ID
        if (active_timeouts[i]->index == id) {
            return i;  // Found it, return the array index
        }
    }

    // Timeout not found in active array
    return -1;
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Clarified the purpose of the return values
- Maintained the original algorithm and logic flow
- Used more descriptive variable declaration style