# dsm_impl_sysv

## Location
src/backend/storage/ipc/dsm_impl.c: 423 - 609

## Overview
System V shared memory implementation for PostgreSQL's dynamic shared memory system, using shmget(), shmat(), shmdt(), and shmctl() for cross-process shared memory segments.

## Definition

```c
struct shmid_ds shm;
```
## Detailed Description
The  function implements dynamic shared memory operations using System V IPC primitives. Unlike POSIX shared memory which uses names, System V shared memory uses integer keys derived from the dsm_handle. The implementation includes careful key management to handle type differences between dsm_handle and key_t, and uses impl_private to cache the shared memory identifier to avoid repeated shmget() lookups.

Key features:
- Converts dsm_handle to key_t, handling potential type size differences gracefully
- Caches shared memory identifiers in impl_private to optimize repeated operations
- Uses IPC_CREAT | IPC_EXCL for CREATE operations to prevent race conditions
- Handles the special IPC_PRIVATE key by treating it as already existing
- For ATTACH operations, uses IPC_STAT to determine segment size dynamically

## Parameters / Member Variables
- : The operation to perform (CREATE/ATTACH/DETACH/DESTROY)
- : DSM handle converted to System V key for segment identification  
- : Size for CREATE operations, ignored for others
- : Caches the shared memory identifier to avoid repeated shmget() calls
- : Pointer to current/new mapping address
- : Pointer to current/new mapping size
- : Error logging level for error messages

## Dependencies
- Functions called/Symbols referenced:
  - shmget (obtain shared memory identifier)
  - shmat (attach shared memory segment)
  - shmdt (detach shared memory segment)  
  - shmctl (control shared memory operations, used for IPC_STAT and IPC_RMID)
  - MemoryContextAlloc (allocate cache for identifier)
  - errcode_for_dynamic_shared_memory (error code helper)
- Called from:
  - dsm_impl_op (when dynamic_shared_memory_type is DSM_IMPL_SYSV)

## Notes and Other Information
- System V shared memory has typically lower default allocation limits than POSIX
- Handles potential type size mismatches between dsm_handle and key_t by consistent truncation
- Avoids negative keys for portability by negating them if handle casts to negative
- Uses IPCProtection flags for appropriate permissions on shared memory segments
- For shmget() with existing segments, must pass size as 0 to avoid EINVAL errors
- Comprehensive cleanup on failure paths including removal of newly created segments
- Identifier caching in impl_private improves performance for repeated operations on same segment