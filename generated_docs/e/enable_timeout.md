# enable_timeout

## Location
[src/backend/utils/misc/timeout.c:158-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L158-L209)

## Overview
Enables a specified timeout by inserting it into the active timeouts array in sorted order by finish time.

## Definition
```c
static void enable_timeout(TimeoutId id, TimestampTz now, TimestampTz fin_time, int interval_in_ms)
```

## Detailed Description
This internal helper function enables a timeout by adding it to the active timeouts list. If the timeout is already active, it first removes the existing entry (effectively rescheduling). The function maintains the active timeouts array in sorted order by finish time, with equal finish times sorted by timeout ID priority.

The function performs several operations:
1. Validates that the timeout system is initialized and the timeout has a valid handler
2. Removes any existing active instance of this timeout (rescheduling behavior)
3. Finds the correct insertion position to maintain sorted order
4. Updates the timeout's parameters and inserts it into the active array

## Parameters / Member Variables
- `id`: The TimeoutId of the timeout to enable
- `now`: Current timestamp when the timeout is being enabled
- `fin_time`: The timestamp when the timeout should fire
- `interval_in_ms`: The interval in milliseconds (for repeating timeouts)

## Dependencies
- Functions called/Symbols referenced:
  - [TimeoutId](../T/TimeoutId.md) (data type)
  - TimestampTz (data type)
  - [find_active_timeout](../f/find_active_timeout.md)
  - [remove_timeout_index](../r/remove_timeout_index.md)
  - [timeout_params](../t/timeout_params.md)
  - [insert_timeout](../i/insert_timeout.md)
  - Assert (for debugging assertions)
- Called from (representative examples):
  - [handle_sig_alarm](../h/handle_sig_alarm.md)
  - [enable_timeout_after](enable_timeout_after.md)
  - [enable_timeout_every](enable_timeout_every.md)
  - [enable_timeout_at](enable_timeout_at.md)
  - [enable_timeouts](enable_timeouts.md)

## Notes and Other Information
- This is a static function internal to the timeout.c module
- Automatically handles rescheduling if the timeout is already active
- Maintains sorted order in the active timeouts array (by finish time, then by ID)
- Sets the timeout indicator to false (will be set true when timeout fires)
- Updates all timeout parameters including start time, finish time, and interval
- It is the caller's responsibility to protect this function from signal handler interruption
- Part of the internal helper functions for PostgreSQL's timeout management system
- Used by various public timeout scheduling functions

## Simplified Source

```c
// Simplified version of enable_timeout
static void enable_timeout(TimeoutId id, TimestampTz now, TimestampTz fin_time, int interval_in_ms) {
    // Validate timeout system and handler are ready
    Assert(all_timeouts_initialized);
    Assert(all_timeouts[id].timeout_handler != NULL);

    // If timeout already active, remove it (reschedule behavior)
    if (all_timeouts[id].active) {
        remove_timeout_index(find_active_timeout(id));
    }

    // Find insertion position to maintain sorted order (by finish time, then by ID)
    int insert_pos = 0;
    for (int i = 0; i < num_active_timeouts; i++) {
        timeout_params *existing = active_timeouts[i];

        if (fin_time < existing->fin_time ||
            (fin_time == existing->fin_time && id < existing->index)) {
            insert_pos = i;
            break;
        }
        insert_pos = i + 1;
    }

    // Configure timeout parameters
    all_timeouts[id].indicator = false;
    all_timeouts[id].start_time = now;
    all_timeouts[id].fin_time = fin_time;
    all_timeouts[id].interval_in_ms = interval_in_ms;

    // Insert timeout into active list at correct position
    insert_timeout(id, insert_pos);
}
```

Key simplifications made:
- Consolidated loop logic for finding insertion position
- Combined loop exit conditions for better readability
- Added clearer variable names (insert_pos, existing)
- Preserved essential algorithm: validation, reschedule handling, sorted insertion
- Maintained all critical functionality while improving code clarity