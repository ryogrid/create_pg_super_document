# insert_timeout

## Location
[src/backend/utils/misc/timeout.c:114-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L114-L136)

## Overview
Inserts a specified timeout into the active timeouts array at the given index position.

## Definition
```c
static void insert_timeout(TimeoutId id, int index)
```

## Detailed Description
This internal helper function inserts a timeout entry into the `active_timeouts` array at the specified index position. The function performs bounds checking to ensure the index is valid, marks the timeout as active in the `all_timeouts` array, and shifts existing entries to make room for the new timeout.

The function maintains the ordered structure of the active timeouts array by shifting all entries from the insertion point rightward by one position. This preserves any ordering that may be significant for timeout scheduling.

## Parameters / Member Variables
- `id`: The TimeoutId of the timeout to insert into the active array
- `index`: The position in the active_timeouts array where the timeout should be inserted

## Dependencies
- Functions called/Symbols referenced:
  - TimeoutId (data type)
  - elog (for error reporting)
  - Assert (for debugging assertions)
- Called from (representative examples):
  - enable_timeout

## Notes and Other Information
- This is a static function internal to the timeout.c module
- Performs bounds checking and will call elog(FATAL) if index is out of range
- Uses Assert to verify the timeout is not already marked as active
- Shifts existing entries in the array to accommodate the new timeout
- Increments the global `num_active_timeouts` counter
- It is the caller's responsibility to protect this function from signal handler interruption
- Part of the internal helper functions for PostgreSQL's timeout management system