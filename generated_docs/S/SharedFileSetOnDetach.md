# SharedFileSetOnDetach

## Location
[src/backend/storage/file/sharedfileset.c:96-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/sharedfileset.c#L96-L114)

## Overview
A cleanup callback function that automatically manages shared file set lifecycle when processes detach from DSM segments, ensuring files are deleted when the last process exits.

## Definition
```c
static void SharedFileSetOnDetach(dsm_segment *segment, Datum datum)
```

## Detailed Description
SharedFileSetOnDetach is a callback function registered with the Dynamic Shared Memory (DSM) subsystem to handle automatic cleanup when processes detach from DSM segments containing SharedFileSets. This function implements reference counting to track how many processes are still using the shared file set and performs cleanup when the last process detaches.

The function operates as follows:
1. Atomically decrements the reference count under spinlock protection
2. Checks if this is the last process to detach (refcnt becomes 0)
3. If it's the last process, initiates deletion of all files in the set
4. Uses safe error handling appropriate for cleanup contexts

This callback is crucial for automatic resource management in parallel query execution, ensuring that temporary files don't accumulate on disk when parallel operations complete or fail. The function is designed to be robust and cannot raise errors since it runs in error cleanup paths.

## Parameters / Member Variables
- `segment`: Pointer to the DSM segment being detached from (not directly used in the function)
- `datum`: A Datum containing the pointer to the SharedFileSet structure being managed

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - SpinLockAcquire
  - SpinLockRelease
  - Assert
  - [FileSetDeleteAll](../F/FileSetDeleteAll.md)
- Called from (representative examples):
  - [SharedFileSetInit](SharedFileSetInit.md) (registered as callback)
  - [SharedFileSetAttach](SharedFileSetAttach.md) (registered as callback)

## Notes and Other Information
- This function is declared static and is only used internally within the sharedfileset.c module
- The function implements the "last one out turns off the lights" pattern for resource cleanup
- Error handling is intentionally minimal since this runs during error cleanup and cannot throw exceptions
- The spinlock ensures atomic reference count operations even under high concurrency
- The callback is automatically invoked by the DSM subsystem when processes detach, either normally or during error recovery
- This design ensures that shared temporary files are always cleaned up, preventing disk space leaks in parallel query scenarios
- The function safely accesses the SharedFileSet data because the callback runs before the actual DSM segment is destroyed

## Simplified Source
```c
static void SharedFileSetOnDetach(dsm_segment *segment, Datum datum)
{
    bool unlink_all = false;
    SharedFileSet *fileset = (SharedFileSet *) DatumGetPointer(datum);

    // Atomically decrement reference count
    SpinLockAcquire(&fileset->mutex);
    Assert(fileset->refcnt > 0);
    if (--fileset->refcnt == 0)
        unlink_all = true;
    SpinLockRelease(&fileset->mutex);

    // Delete all files if this was the last process using the file set
    if (unlink_all)
        FileSetDeleteAll(&fileset->fs);
}
```