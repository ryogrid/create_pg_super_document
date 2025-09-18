# filter_init

## Location
[src/bin/pg_dump/filter.c:37-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/filter.c#L37-L60)

## Overview
Initializes a filter state structure for reading filter files used by pg_dump, pg_dumpall, and pg_restore utilities.

## Definition
```c
void filter_init(FilterStateData *fstate, const char *filename, exit_function f_exit)
```

## Detailed Description
This function initializes a FilterStateData structure for processing filter files. It opens the specified filter file for reading and sets up the necessary state information. The function supports reading from either a named file or stdin (when filename is "-"). It also initializes a string buffer for line processing and sets up an application-specific exit function for error handling.

## Parameters / Member Variables
- `fstate`: Pointer to FilterStateData structure to be initialized
- `filename`: Path to the filter file to open, or "-" to read from stdin
- `f_exit`: Function pointer to application-specific exit function for error handling

## Dependencies
- Functions called/Symbols referenced:
  - fopen
  - initStringInfo
  - pg_log_error
  - strcmp
- Called from (representative examples):
  - [read_dump_filters](../r/read_dump_filters.md) (in pg_dump.c)
  - [read_dumpall_filters](../r/read_dumpall_filters.md) (in pg_dumpall.c)
  - [read_restore_filters](../r/read_restore_filters.md) (in pg_restore.c)

## Notes and Other Information
- The function is part of the common filter infrastructure shared between pg_dump utilities
- Error handling is delegated to the application-specific exit function passed as parameter
- The function initializes the line number counter to 0 and sets up a string buffer for line processing
- When filename is "-", the function uses stdin instead of opening a file