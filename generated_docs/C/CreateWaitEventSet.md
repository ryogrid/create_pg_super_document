# CreateWaitEventSet

## Location
[src/backend/storage/ipc/latch.c:751-873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L751-L873)

## Overview
CreateWaitEventSet allocates and initializes a WaitEventSet structure that can efficiently wait for multiple types of events simultaneously using platform-specific event notification mechanisms.

## Definition

```c
struct epoll_event) * nevents);
```
## Detailed Description
CreateWaitEventSet creates a WaitEventSet structure capable of monitoring multiple events efficiently using the best available platform-specific mechanism (epoll on Linux, kqueue on BSD systems, poll on other Unix systems, or Win32 events on Windows). The function allocates a single contiguous block of memory containing the WaitEventSet structure, an array of WaitEvent structures, and platform-specific event structures.

The function uses MAXALIGN to ensure proper memory alignment for all data structures, which is particularly important for platform-specific structures like epoll_event that may have strict alignment requirements. The allocated memory is zero-initialized and managed by the TopMemoryContext to ensure it persists across transaction boundaries.

Platform-specific initialization includes creating file descriptors for epoll/kqueue systems, setting up poll structures for poll-based systems, or initializing event handles for Windows. The function also handles resource ownership tracking and file descriptor management to ensure proper cleanup.

## Parameters / Member Variables
- : ResourceOwner to track this WaitEventSet for automatic cleanup (NULL for session lifetime)
- : Maximum number of events this set can simultaneously monitor

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (memory allocation)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)/ResourceOwnerRememberWaitEventSet (resource tracking)
  - [AcquireExternalFD](../A/AcquireExternalFD.md)/ReleaseExternalFD (file descriptor management)
  - epoll_create1 (Linux epoll initialization)
  - kqueue/fcntl (BSD kqueue initialization)
  - pgwin32_signal_event (Windows signal handling)
- Called from (representative examples):
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md) (latch waiting with socket support)
  - [InitializeLatchWaitSet](../I/InitializeLatchWaitSet.md) (shared latch wait set initialization)
  - [ConfigurePostmasterWaitSet](ConfigurePostmasterWaitSet.md) (postmaster event monitoring)
  - [ExecAppendAsyncEventWait](../E/ExecAppendAsyncEventWait.md) (async append execution)

## Notes and Other Information
- Uses platform-specific event notification mechanisms for optimal performance on each operating system
- Memory is allocated as a single contiguous block with proper alignment for all contained structures
- On Windows, the first event handle is reserved for pgwin32_signal_event to ensure signal processing priority
- File descriptor limits are respected through AcquireExternalFD/ReleaseExternalFD on Unix systems
- The structure supports resource owner tracking for automatic cleanup when transactions end
- CLOEXEC flags are set on Unix file descriptors to prevent inheritance by child processes
- Error handling includes proper cleanup of partially-initialized resources

## Simplified Source

```c
// Simplified version of CreateWaitEventSet
WaitEventSet *
CreateWaitEventSet(ResourceOwner resowner, int nevents)
{
    WaitEventSet *set;
    Size total_size = 0;

    // Calculate total memory needed for all structures
    total_size += MAXALIGN(sizeof(WaitEventSet));
    total_size += MAXALIGN(sizeof(WaitEvent) * nevents);

    // Add platform-specific structure sizes
    total_size += platform_specific_size(nevents);

    // Ensure resource owner can track this allocation
    if (resowner != NULL) {
        ResourceOwnerEnlarge(resowner);
    }

    // Allocate contiguous memory block in top context
    char *data = MemoryContextAllocZero(TopMemoryContext, total_size);

    // Set up main structure and event array
    set = (WaitEventSet *) data;
    data += MAXALIGN(sizeof(WaitEventSet));
    set->events = (WaitEvent *) data;
    data += MAXALIGN(sizeof(WaitEvent) * nevents);

    // Initialize platform-specific structures
    setup_platform_structures(set, data, nevents);

    // Initialize basic fields
    set->latch = NULL;
    set->nevents_space = nevents;
    set->exit_on_postmaster_death = false;

    // Register with resource owner for cleanup
    if (resowner != NULL) {
        ResourceOwnerRememberWaitEventSet(resowner, set);
        set->owner = resowner;
    }

    // Create platform-specific event mechanism
    initialize_event_mechanism(set);

    return set;
}
```

Key simplifications made:
- Abstracted platform-specific memory calculations into `platform_specific_size()`
- Consolidated platform-specific structure setup into `setup_platform_structures()`
- Simplified event mechanism initialization into `initialize_event_mechanism()`
- Removed detailed error handling for clarity
- Focused on the main allocation and initialization flow
- Maintained the essential algorithm: calculate size, allocate memory, set up structures, initialize fields