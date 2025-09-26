# count_usable_fds

## Location
[src/backend/storage/file/fd.c:961-1040](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L961-L1040)

## Overview
Counts how many file descriptors the system will allow PostgreSQL to open and estimates how many are already in use by systematically attempting to duplicate file descriptors.

## Definition
```c
static void count_usable_fds(int max_to_probe, int *usable_fds, int *already_open)
```

## Detailed Description
This function performs file descriptor availability testing by repeatedly calling `dup(2)` (duplicating stderr) until it fails or reaches the specified probe limit. It uses this approach to determine both the practical limit of file descriptors the process can open and estimate how many are currently in use.

The function employs a systematic probing strategy:
1. Attempts to duplicate stderr (FD 2) repeatedly
2. Tracks the highest file descriptor number achieved
3. Stops when `dup()` fails (typically with EMFILE/ENFILE) or max_to_probe is reached
4. Calculates usable FDs and estimates already-open FDs based on the gap between highest FD and successful duplications

The function respects system limits by checking RLIMIT_NOFILE when available and avoids going beyond the hard limit to prevent kernel log spam.

## Parameters / Member Variables
- `max_to_probe`: Maximum number of file descriptors to test before stopping the probe
- `usable_fds`: Output parameter - number of file descriptors successfully duplicated (available for use)
- `already_open`: Output parameter - estimated number of file descriptors already in use by the process

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - [pfree](../p/pfree.md)
  - dup
  - close
  - getrlimit (when HAVE_GETRLIMIT is defined)
  - ereport
  - elog
- Called from (representative examples):
  - [set_max_safe_fds](../s/set_max_safe_fds.md)

## Notes and Other Information
- Assumes stderr (FD 2) is available for duplication - closing stderr would break this function
- Uses dynamic memory allocation to store duplicated FDs, starting with 1024 entries and doubling as needed
- The calculation `already_open = highestfd + 1 - used` assumes FDs are numbered starting from 0
- Small values of max_to_probe may underestimate already_open due to gaps in the FD space
- Function is static and only used internally within the file descriptor management subsystem
- Handles platform differences through conditional compilation (HAVE_GETRLIMIT)