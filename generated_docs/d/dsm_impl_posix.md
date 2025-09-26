# dsm_impl_posix

## Location
src/backend/storage/ipc/dsm_impl.c: 212 - 350

## Overview
POSIX shared memory implementation for PostgreSQL's dynamic shared memory system, using shm_open() and memory mapping for cross-process shared memory segments.

## Definition


## Detailed Description
The  function implements dynamic shared memory operations using POSIX shared memory primitives. It creates shared memory segments using  and maps them into the process address space using . The implementation uses a naming convention  for shared memory objects.

Key operations handled:
- **DSM_OP_CREATE**: Creates a new shared memory segment, sizes it, and maps it
- **DSM_OP_ATTACH**: Opens an existing segment, determines its size, and maps it  
- **DSM_OP_DETACH**: Unmaps the segment from process address space
- **DSM_OP_DESTROY**: Unmaps and removes the shared memory segment entirely

The function includes comprehensive error handling and cleanup for all failure scenarios, ensuring resources are properly released even when operations fail partway through.

## Parameters / Member Variables
- : The operation to perform (CREATE/ATTACH/DETACH/DESTROY)
- : Unique identifier used to generate the shared memory segment name
- : Size for CREATE operations, ignored for others
- : Implementation-specific private data (unused in POSIX implementation)
- : Pointer to current/new mapping address
- : Pointer to current/new mapping size  
- : Error logging level for error messages

## Dependencies
- Functions called/Symbols referenced:
  - shm_open (POSIX shared memory creation)
  - shm_unlink (POSIX shared memory removal)
  - mmap/munmap (memory mapping operations)
  - fstat (file statistics for size determination)
  - dsm_impl_posix_resize (segment resizing)
  - ReserveExternalFD/ReleaseExternalFD (file descriptor management)
  - errcode_for_dynamic_shared_memory (error code helper)
- Called from:
  - dsm_impl_op (when dynamic_shared_memory_type is DSM_IMPL_POSIX)

## Notes and Other Information
- Uses file descriptor reservation to prevent EMFILE errors during segment operations
- Segment names follow pattern  in POSIX shared memory namespace
- On some platforms, POSIX shared memory may be implemented as files in filesystem
- Includes platform-specific mmap flags (MAP_HASSEMAPHORE, MAP_NOSYNC) for optimization
- For CREATE operations, uses O_CREAT | O_EXCL flags to prevent race conditions
- Comprehensive error handling with proper cleanup on all failure paths
- File descriptors are closed immediately after mapping to minimize resource usage