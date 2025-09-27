# remove_timeout_index

## Location
[src/backend/utils/misc/timeout.c:137-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L137-L157)

## Overview
Removes a timeout entry from the active timeouts array at the specified index position.

## Definition
```c
static void remove_timeout_index(int index)
```

## Detailed Description
This internal helper function removes a timeout entry from the `active_timeouts` array at the specified index position. The function performs bounds checking to ensure the index is valid, marks the timeout as inactive in the `all_timeouts` array, and shifts remaining entries to fill the gap left by the removed timeout.

The function maintains the contiguous structure of the active timeouts array by shifting all entries after the removal point leftward by one position. This preserves any ordering that may be significant for timeout scheduling.

## Parameters / Member Variables
- `index`: The position in the active_timeouts array of the timeout to remove

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
  - Assert (for debugging assertions)
- Called from (representative examples):
  - [enable_timeout](../e/enable_timeout.md)
  - [handle_sig_alarm](../h/handle_sig_alarm.md)
  - [disable_timeout](../d/disable_timeout.md)
  - [disable_timeouts](../d/disable_timeouts.md)

## Notes and Other Information
- This is a static function internal to the timeout.c module
- Performs bounds checking and will call elog(FATAL) if index is out of range
- Uses Assert to verify the timeout at the specified index is currently marked as active
- Shifts remaining entries in the array to fill the gap left by removal
- Decrements the global `num_active_timeouts` counter
- It is the caller's responsibility to protect this function from signal handler interruption
- Part of the internal helper functions for PostgreSQL's timeout management system
- Called from various timeout management operations including signal handling

## Simplified Source

```c
// Simplified version of remove_timeout_index
static void remove_timeout_index(int index) {
    // Validate index bounds
    if (index < 0 || index >= num_active_timeouts) {
        elog(FATAL, "timeout index %d out of range 0..%d", index,
             num_active_timeouts - 1);
    }

    // Mark timeout as inactive
    Assert(active_timeouts[index]->active);
    active_timeouts[index]->active = false;

    // Shift remaining timeouts left to fill the gap
    for (int i = index + 1; i < num_active_timeouts; i++) {
        active_timeouts[i - 1] = active_timeouts[i];
    }

    // Update active timeout count
    num_active_timeouts--;
}
```

Key simplifications made:
- Moved variable declaration inline in the for loop for clarity
- Added descriptive comments for each main operation
- Preserved all essential logic including bounds checking and error handling
- Maintained the core array manipulation algorithm