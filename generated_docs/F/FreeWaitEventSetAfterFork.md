# FreeWaitEventSetAfterFork

## Location
[src/backend/storage/ipc/latch.c:917-932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L917-L932)

## Overview
Frees a previously created WaitEventSet in a child process after a fork(), properly cleaning up platform-specific resources that should not be inherited.

## Definition

```c
void
FreeWaitEventSetAfterFork(WaitEventSet *set)
```

## Detailed Description
This function is specifically designed to handle the cleanup of WaitEventSet resources in child processes after a fork() system call. It performs platform-specific cleanup operations based on the wait mechanism in use:

- On epoll-based systems (Linux), it closes the epoll file descriptor and releases the external FD tracking
- On kqueue-based systems (BSD variants), it only releases the external FD tracking since kqueues are not normally inherited by child processes
- Finally, it frees the WaitEventSet structure itself

This function is essential for proper resource management in PostgreSQL's process model, ensuring that child processes don't hold onto file descriptors or other resources that should remain exclusive to the parent process.

## Parameters / Member Variables
- `set`: Pointer to the WaitEventSet structure to be freed after fork

## Dependencies
- Functions called/Symbols referenced:
  - close (system call, epoll systems only)
  - [ReleaseExternalFD](../R/ReleaseExternalFD.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ClosePostmasterPorts](../C/ClosePostmasterPorts.md)

## Notes and Other Information
- This function should only be called in child processes after a fork(), not in the original parent process
- The function uses conditional compilation to handle different wait mechanisms (epoll vs kqueue)
- On kqueue systems, the kqueue file descriptor cleanup is not needed because kqueues are not inherited by default
- This is part of PostgreSQL's careful resource management during process forking in the postmaster

## Simplified Source

```c
// Simplified version of FreeWaitEventSetAfterFork
void FreeWaitEventSetAfterFork(WaitEventSet *set) {
#if defined(WAIT_USE_EPOLL)
    // On epoll systems: close the epoll file descriptor
    close(set->epoll_fd);
    ReleaseExternalFD();
#elif defined(WAIT_USE_KQUEUE)
    // On kqueue systems: kqueues aren't inherited, just release FD tracking
    ReleaseExternalFD();
#endif

    // Free the WaitEventSet structure
    pfree(set);
}
```

Key simplifications made:
- Added clear comments for platform-specific cleanup
- Preserved the essential conditional compilation logic
- Maintained the proper cleanup order (FD operations before freeing)
- Function is already very simple and focused