# SharedFileSetAttach

## Location
src/backend/storage/file/sharedfileset.c: 56 - 82

## Overview
Attaches a backend process to an existing shared file set, incrementing the reference count to track active users and registering cleanup callbacks.

## Definition
```c
void SharedFileSetAttach(SharedFileSet *fileset, dsm_segment *seg)
```

## Detailed Description
SharedFileSetAttach allows a backend process to attach to a SharedFileSet that was previously created by SharedFileSetInit. The function performs atomic reference count management using spinlocks to ensure thread-safety in parallel execution environments. It increments the reference count to track how many processes are currently using the shared file set and registers a cleanup callback to handle proper resource deallocation when the process detaches.

The function includes error checking to prevent attachment to destroyed file sets. If the reference count is already zero (indicating the file set has been destroyed), the function raises an error rather than allowing invalid access.

Key operations performed:
1. Atomically checks and increments the reference count under spinlock protection
2. Validates that the file set is still active (refcnt > 0)
3. Registers a cleanup callback with the DSM segment for proper resource management
4. Reports an error if attempting to attach to a destroyed file set

## Parameters / Member Variables
- `fileset`: Pointer to the SharedFileSet structure to attach to
- `seg`: Pointer to the DSM segment associated with this shared file set for cleanup registration

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - ereport
  - errcode
  - errmsg
  - on_dsm_detach
  - SharedFileSetOnDetach
  - PointerGetDatum
- Called from (representative examples):
  - ExecHashJoinInitializeWorker (Hash join worker process initialization)
  - tuplesort_attach_shared (Shared tuplesort worker attachment)

## Notes and Other Information
- This function is essential for parallel query execution where worker processes need to access shared temporary files created by the leader process
- The spinlock-based reference counting ensures atomic operations even under high concurrency
- Error handling prevents race conditions where a process might try to attach to a file set that's being destroyed
- The cleanup callback ensures that when this process detaches from the DSM segment, proper cleanup occurs
- This is typically called by worker processes in parallel queries to gain access to shared temporary storage