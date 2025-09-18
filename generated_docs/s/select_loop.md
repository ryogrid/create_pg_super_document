# select_loop

## Location
src/fe_utils/parallel_slot.c: 80 - 134

## Overview
A static function that waits for file descriptors in a given set to become readable, handling interrupts gracefully across different platforms.

## Definition
```c
static int select_loop(int maxFd, fd_set *workerset)
```

## Detailed Description
This function implements a robust wrapper around the system `select()` call that waits for any file descriptor in the provided set to become readable. It handles platform-specific interrupt behavior by automatically retrying the select operation when interrupted by signals (EINTR on Unix/Linux, WSAEINTR on Windows). The function preserves the original file descriptor set by making a copy before each select call, ensuring that the set can be reused for subsequent operations. This is essential for parallel processing scenarios where multiple file descriptors need to be monitored simultaneously.

## Parameters / Member Variables
- `maxFd`: The highest-numbered file descriptor in the set plus one, as required by the select() system call
- `workerset`: A pointer to the fd_set containing file descriptors to monitor for readability

## Dependencies
- Functions called/Symbols referenced:
  - select (system call for I/O multiplexing)
  - EINTR (error code for interrupted system calls)
- Called from (representative examples):
  - [getMessageFromWorker](../g/getMessageFromWorker.md)
  - [wait_on_slots](../w/wait_on_slots.md)

## Notes and Other Information
- This is a static function, only accessible within the parallel.c file
- Implements cross-platform interrupt handling (Unix EINTR vs Windows WSAEINTR)
- The function modifies the workerset during select(), so it preserves the original by copying it each iteration
- Returns -1 on error, or the number of readable descriptors on success
- Used primarily in pg_dump's parallel processing infrastructure for coordinating worker communication
- Essential for reliable I/O multiplexing in multi-process database dump operations