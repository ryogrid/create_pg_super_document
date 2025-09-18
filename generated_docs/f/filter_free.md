# filter_free

## Location
[src/bin/pg_dump/filter.c:61-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/filter.c#L61-L82)

## Overview
Releases allocated resources and properly closes files associated with a filter state structure.

## Definition
```c
void filter_free(FilterStateData *fstate)
```

## Detailed Description
This function performs cleanup operations for a FilterStateData structure that was previously initialized with filter_init. It safely releases the memory allocated for the line buffer and closes the associated file handle if it's not stdin. The function includes proper error checking when closing files and handles NULL pointer safety.

## Parameters / Member Variables
- `fstate`: Pointer to FilterStateData structure to be cleaned up (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - free
  - fclose
  - pg_log_error
- Called from (representative examples):
  - [read_dump_filters](../r/read_dump_filters.md) (in pg_dump.c)
  - [read_dumpall_filters](../r/read_dumpall_filters.md) (in pg_dumpall.c)
  - [read_restore_filters](../r/read_restore_filters.md) (in pg_restore.c)

## Notes and Other Information
- The function is NULL-safe and returns early if fstate is NULL
- Only closes the file pointer if it's not stdin to avoid closing the standard input stream
- Errors during file closure are logged but do not cause the program to exit
- Part of the cleanup phase in the filter infrastructure used by pg_dump utilities
- Sets the file pointer to NULL after successful closure for safety