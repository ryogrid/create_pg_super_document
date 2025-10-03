# InitLatch

## Location
[src/backend/storage/ipc/latch.c:394-429](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L394-L429)

## Overview
Initializes a process-local latch structure, setting up the necessary platform-specific mechanisms for inter-process synchronization within a single process.

## Definition

```c
void
InitLatch(Latch *latch)
```
## Detailed Description
InitLatch initializes a process-local latch by setting up its basic state and platform-specific synchronization mechanisms. The function sets the latch as unset, marks it as not sleeping, assigns the current process PID as the owner, and indicates it's not shared between processes. Depending on the platform compilation flags, it either verifies that the self-pipe or signalfd mechanisms are properly initialized, or on Windows, creates a new event object for synchronization.

## Parameters / Member Variables
- `*latch`: Pointer to the Latch structure to be initialized
## Dependencies
- Functions called/Symbols referenced:
  - [Latch](../L/Latch.md) (structure type)
  - WAIT_USE_SELF_PIPE (conditional compilation flag)
  - WAIT_USE_SIGNALFD (conditional compilation flag)
  - WAIT_USE_WIN32 (conditional compilation flag)
- Called from (representative examples):
  - [InitProcessLocalLatch](InitProcessLocalLatch.md)

## Notes and Other Information
- The function assumes that InitializeLatchSupport has been called previously in the current process on Unix-like systems
- On Windows, it creates a manual-reset event object that starts in non-signaled state
- The latch is marked as process-local (not shared between processes) and owned by the current process
- Platform-specific assertions ensure proper initialization of underlying synchronization mechanisms

## Simplified Source

```c
// Simplified version of InitLatch
void InitLatch(Latch *latch) {
    // Initialize basic latch state
    latch->is_set = false;
    latch->maybe_sleeping = false;
    latch->owner_pid = MyProcPid;
    latch->is_shared = false;

    // Platform-specific initialization
#if defined(WAIT_USE_SELF_PIPE)
    // Verify self-pipe mechanism is ready
    Assert(selfpipe_readfd >= 0 && selfpipe_owner_pid == MyProcPid);
#elif defined(WAIT_USE_SIGNALFD)
    // Verify signalfd mechanism is ready
    Assert(signal_fd >= 0);
#elif defined(WAIT_USE_WIN32)
    // Create Windows event object for signaling
    latch->event = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (latch->event == NULL)
        elog(ERROR, "CreateEvent failed: error code %lu", GetLastError());
#endif
}
```

Key simplifications made:
- Added descriptive comments for each initialization step
- Grouped platform-specific code with clear section comment
- Simplified conditional compilation comments to explain purpose
- Maintained all essential logic and error handling
- Preserved the core algorithm without losing functionality