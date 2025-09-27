# FreeWaitEventSet

## Location
[src/backend/storage/ipc/latch.c:874-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L874-L916)

## Overview
FreeWaitEventSet properly cleans up and deallocates a WaitEventSet structure, releasing all associated platform-specific resources and memory.

## Definition

```c
void
FreeWaitEventSet(WaitEventSet *set)
```
## Detailed Description
FreeWaitEventSet performs comprehensive cleanup of a WaitEventSet structure, ensuring that all platform-specific resources are properly released. The function handles resource owner tracking, closes file descriptors on Unix systems, and cleans up event objects on Windows.

The cleanup process is carefully designed to avoid resource leaks across exec() calls. On Unix systems, the epoll/kqueue file descriptors are created with CLOEXEC flags to prevent inheritance. On Windows, the function assumes that event handles are non-inheritable and performs explicit cleanup of WSA event objects created for socket monitoring.

Resource tracking is handled through the ResourceOwner system, which allows automatic cleanup when transactions abort or when the session ends. The function removes the WaitEventSet from its owner's tracking before proceeding with platform-specific cleanup.

## Parameters / Member Variables
- `set`: Pointer to the WaitEventSet structure to free and clean up

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForgetWaitEventSet](../R/ResourceOwnerForgetWaitEventSet.md) (resource tracking cleanup)
  - close (Unix file descriptor cleanup)
  - [ReleaseExternalFD](../R/ReleaseExternalFD.md) (file descriptor quota management)
  - WSAEventSelect/WSACloseEvent (Windows socket event cleanup)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md) (temporary wait set cleanup)
  - [ShutdownLatchSupport](../S/ShutdownLatchSupport.md) (system shutdown cleanup)
  - [ResOwnerReleaseWaitEventSet](../R/ResOwnerReleaseWaitEventSet.md) (resource owner cleanup)
  - [ExecAppendAsyncEventWait](../E/ExecAppendAsyncEventWait.md) (async execution cleanup)

## Notes and Other Information
- Designed to be safe across exec() calls by using CLOEXEC flags and non-inheritable handles
- Handles platform-specific cleanup for epoll (Linux), kqueue (BSD), poll (Unix), and Win32 events
- On Windows, distinguishes between different event types (latch, postmaster death, socket) for proper cleanup
- For socket events on Windows, explicitly calls WSAEventSelect and WSACloseEvent to clean up WSA event objects
- Resource tracking ensures automatic cleanup when transactions abort or sessions end
- The function is idempotent - it's safe to call multiple times on the same set
- Memory is freed using pfree, which works with PostgreSQL's memory context system

## Simplified Source

```c
// Simplified version of FreeWaitEventSet
void FreeWaitEventSet(WaitEventSet *set) {
    // Step 1: Remove from resource owner tracking if present
    if (set->owner) {
        ResourceOwnerForgetWaitEventSet(set->owner, set);
        set->owner = NULL;
    }

    // Step 2: Platform-specific cleanup of file descriptors/handles
#if defined(WAIT_USE_EPOLL)
    // Linux: Close epoll file descriptor
    close(set->epoll_fd);
    ReleaseExternalFD();

#elif defined(WAIT_USE_KQUEUE)
    // BSD: Close kqueue file descriptor
    close(set->kqueue_fd);
    ReleaseExternalFD();

#elif defined(WAIT_USE_WIN32)
    // Windows: Clean up event objects for each monitored event
    for (WaitEvent *event = set->events; event < (set->events + set->nevents); event++) {
        if (event->events & (WL_LATCH_SET | WL_POSTMASTER_DEATH)) {
            // Special events use shared handles - no cleanup needed
        } else {
            // Socket events: clean up WSA event objects
            WSAEventSelect(event->fd, NULL, 0);
            WSACloseEvent(set->handles[event->pos + 1]);
        }
    }
#endif

    // Step 3: Free the WaitEventSet structure itself
    pfree(set);
}
```

Key simplifications made:
- Removed detailed comments about resource inheritance across exec() calls
- Consolidated the Windows event cleanup loop logic
- Abstracted platform-specific details with clear section comments
- Focused on the three main steps: resource tracking, platform cleanup, memory deallocation
- Simplified variable names and removed complex iterator expressions