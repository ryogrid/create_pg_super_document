# dsm_attach

## Location
[src/backend/storage/ipc/dsm.c:665-756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L665-L756)

## Overview
Attaches to an existing dynamic shared memory segment using its handle, incrementing the reference count and mapping the segment into the current process's address space.

## Definition

```c
dsm_segment *
dsm_attach(dsm_handle h)
```
## Detailed Description
The  function provides the mechanism for attaching to an existing DSM segment that was previously created by another process. It performs several critical operations to ensure safe and correct attachment:

1. **Duplicate attachment prevention**: Checks if the segment is already attached to the current process to prevent multiple attachments
2. **Handle lookup**: Searches the DSM control segment to find an active slot with the matching handle
3. **Reference counting**: Increments the segment's reference count to prevent premature destruction
4. **Memory mapping**: Maps the segment into the current process's address space, either from the main shared memory region or via platform-specific operations

The function handles both main region segments (allocated from PostgreSQL's main shared memory) and system-level segments (created via OS-specific mechanisms). It includes robust error handling for cases where the segment may have been destroyed between the time the handle was obtained and the attachment attempt.

## Parameters / Member Variables
- : The DSM handle identifying the segment to attach to, typically obtained from another process or a persistent store

## Dependencies
- Functions called/Symbols referenced:
  - dsm_backend_startup (initialization if needed)
  - [dsm_create_descriptor](dsm_create_descriptor.md) (creates local segment descriptor)
  - dlist_foreach/dlist_container (iterates through attached segments)
  - [is_main_region_dsm_handle](../i/is_main_region_dsm_handle.md) (checks if handle is for main region)
  - dsm_impl_op (platform-specific attachment operations)
  - [dsm_detach](dsm_detach.md) (cleanup on failure)
- Called from (representative examples):
  - [AttachSession](../A/AttachSession.md) (session management)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (parallel query workers)
  - [dsa_attach](dsa_attach.md) (dynamic shared arrays)
  - GetNamedDSMSegment (named segment registry)

## Notes and Other Information
- Only safe to call under postmaster (assertion enforced)
- Returns NULL if segment not found or already destroyed
- Prevents duplicate attachments to the same segment within a process
- Automatically integrates with CurrentResourceOwner for cleanup tracking
- Reference count must be > 1 for valid attachment (count of 1 indicates pending destruction)
- Handles both main region and system-level segment types transparently
- Thread-safe operation using DynamicSharedMemoryControlLock
- Caller should use dsm_find_mapping() first to check for existing attachments