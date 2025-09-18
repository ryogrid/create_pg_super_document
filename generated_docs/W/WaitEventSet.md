# WaitEventSet

## Location
src/backend/storage/ipc/latch.c: 102 - 157

## Overview
WaitEventSet is a core data structure in PostgreSQL that manages a collection of events that can be waited on simultaneously, providing a platform-independent interface for I/O multiplexing and event-driven programming.

## Definition


## Detailed Description
The WaitEventSet structure is PostgreSQL's abstraction for efficient event waiting across different operating systems. It encapsulates platform-specific I/O multiplexing mechanisms (epoll on Linux, kqueue on BSD systems, poll on other Unix systems, and Windows events on Windows) to provide a unified interface for waiting on multiple file descriptors, sockets, latches, and other events simultaneously.

This structure is fundamental to PostgreSQL's asynchronous I/O operations, enabling the server to efficiently handle multiple concurrent connections and internal events without blocking. It supports various event types including socket readiness, latch signaling, postmaster death detection, and timeouts.

The implementation uses compile-time conditionals to select the most efficient platform-specific mechanism available, ensuring optimal performance across different operating systems while maintaining a consistent API.

## Parameters / Member Variables
- : ResourceOwner that tracks this WaitEventSet for cleanup purposes
- : Current number of registered events in the set
- : Maximum number of events that can be stored (allocated capacity)
- : Array of WaitEvent structures defining the events to wait for
- : Pointer to a latch if WL_LATCH_SET is specified in any wait event
- : Position of the latch event in the events array for quick access
- : Flag to exit immediately when postmaster death is detected
- : File descriptor for epoll instance (Linux-specific)
- : Pre-allocated array for epoll_wait results (Linux-specific)
- : File descriptor for kqueue instance (BSD-specific)
- : Pre-allocated array for kevent results (BSD-specific)
- : Flag for postmaster status reporting (BSD-specific)
- : Array of pollfd structures for poll() system call (generic Unix)
- : Array of Windows event handles (Windows-specific)

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwner
  - WaitEvent
  - Latch
  - WAIT_USE_EPOLL
  - WAIT_USE_KQUEUE
  - WAIT_USE_POLL
  - WAIT_USE_WIN32

- Called from (representative examples):
  - CreateWaitEventSet
  - FreeWaitEventSet
  - AddWaitEventToSet
  - ModifyWaitEvent
  - WaitEventSetWait
  - WaitEventSetWaitBlock
  - GetNumRegisteredWaitEvents
  - SysLoggerMain
  - pq_putmessage_noblock

## Notes and Other Information
- The structure uses conditional compilation to include only the platform-specific members needed for the target operating system
- Memory management is integrated with PostgreSQL's ResourceOwner system for automatic cleanup
- The latch optimization allows quick checking of latch state before expensive syscalls
- Platform-specific event arrays are pre-allocated to avoid repeated memory allocation during wait operations
- The exit_on_postmaster_death feature provides a safety mechanism for child processes to terminate when the postmaster dies
- This is a fundamental building block for PostgreSQL's event-driven architecture and is used extensively throughout the codebase for asynchronous operations