# resetPQExpBuffer

## Location
src/interfaces/libpq/pqexpbuffer.c: 146 - 171

## Overview
Resets a PQExpBuffer to an empty state, clearing its contents and restoring it to a usable condition even if it was previously in a "broken" state.

## Definition
```c
void resetPQExpBuffer(PQExpBuffer str)
```

## Detailed Description
The `resetPQExpBuffer` function resets a PQExpBuffer structure to its empty state. It handles two scenarios:

1. **Normal buffer**: If the buffer's data pointer is not pointing to the special `oom_buffer` (out-of-memory buffer), it simply sets the length to 0 and places a null terminator at the beginning of the data array.

2. **Broken buffer**: If the buffer is in a "broken" state (data pointer points to `oom_buffer`), it attempts to reinitialize the buffer to a valid state by calling `initPQExpBuffer`.

This function is essential for buffer reuse and recovery from error conditions, particularly out-of-memory situations where the buffer may have been put into a special broken state.

## Parameters / Member Variables
- `str`: Pointer to the PQExpBuffer structure to reset. If NULL, the function does nothing.

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
- Called from (representative examples):
  - prepare_heap_command (pg_amcheck)
  - StreamLogicalLog (pg_recvlogical)
  - dumpTableData_insert (pg_dump)
  - exec_command_reset (psql)
  - printfPQExpBuffer (pqexpbuffer)

## Notes and Other Information
- This function is NULL-safe - it checks if the input pointer is valid before proceeding
- The function handles the special case of `oom_buffer`, which is used as a sentinel value when memory allocation fails
- Widely used throughout PostgreSQL client tools and utilities for buffer management
- Part of the libpq interface for expandable string buffers