# drain

## Location
src/backend/storage/ipc/latch.c: 2331 - 2382

## Overview
Reads and discards all available data from the self-pipe or signalfd to clear pending wakeup notifications in PostgreSQL's latch system.

## Definition
```c
static void drain(void)
```

## Detailed Description
The `drain` function is responsible for consuming all pending data from either the self-pipe (traditional Unix approach) or signalfd (Linux-specific approach) used by PostgreSQL's latch implementation. This function is called when a process has been awakened and needs to clear all accumulated wakeup signals to reset the notification mechanism for future use.

The function operates in a loop, continuously reading data in 1024-byte chunks until the descriptor is empty (indicated by `EAGAIN`/`EWOULDBLOCK` errors). This ensures that multiple rapid latch sets don't leave residual data that could cause spurious wakeups.

Key behaviors:
- Uses conditional compilation to work with either self-pipe (`WAIT_USE_SELF_PIPE`) or signalfd mechanisms
- Implements robust error handling with different behaviors for various error conditions
- Resets the global `waiting` flag on fatal errors to maintain system consistency
- Continues reading until the entire descriptor is drained to prevent accumulation

## Parameters / Member Variables
This function takes no parameters and operates on global state:
- Uses `selfpipe_readfd` (when `WAIT_USE_SELF_PIPE` is defined) or `signal_fd` for reading
- Modifies `waiting` global flag on error conditions
- Uses a local 1024-byte buffer for reading data chunks

## Dependencies
- Functions called/Symbols referenced:
  - `read` (system call)
  - `EAGAIN` (errno constant)
  - `EWOULDBLOCK` (errno constant)
  - `EINTR` (errno constant)
  - `elog` (PostgreSQL logging function)
  - `WAIT_USE_SELF_PIPE` (conditional compilation macro)

- Called from (representative examples):
  - `WaitEventSetWaitBlock` - after waking up from blocking wait
  - `LatchWaitSetLatchPos` - during latch position management

## Notes and Other Information
- Only called when the global `waiting` flag is true, indicating an active wait state
- Critical for preventing accumulation of stale wakeup data that could cause spurious wakeups
- The 1024-byte buffer size is chosen to efficiently drain typical amounts of accumulated data
- Error handling includes resetting `waiting` flag to prevent system inconsistency
- Supports both traditional Unix self-pipe and Linux signalfd mechanisms through conditional compilation
- EOF conditions are treated as fatal errors since they indicate unexpected descriptor closure
- The function must completely drain the descriptor to ensure clean state for subsequent waits