# dsm_create

## Location
src/backend/storage/ipc/dsm.c: 516 - 664

## Overview
Creates a new dynamic shared memory segment with specified size and flags, managing the allocation either from the main shared memory region or by creating a new system-level memory segment.

## Definition


## Detailed Description
The  function is the primary interface for creating new dynamic shared memory (DSM) segments in PostgreSQL. It handles the complete lifecycle of segment creation, including memory allocation, control structure management, and reference counting setup.

The function first attempts to allocate space from the main shared memory region if available (using FreePageManager). If that fails or if the main region isn't available, it creates a new system-level memory segment using platform-specific implementations. The function manages the DSM control segment to track all active segments and ensures proper reference counting to prevent premature destruction.

Key behaviors include:
- Automatic initialization of DSM subsystem if not already done
- Preference for main shared memory region allocation when possible
- Fallback to system-level segment creation with collision-resistant handle generation
- Integration with PostgreSQL's resource management system
- Thread-safe operation using DynamicSharedMemoryControlLock

## Parameters / Member Variables
- : The requested size in bytes for the new DSM segment
- : Control flags, including DSM_CREATE_NULL_IF_MAXSEGMENTS to return NULL instead of erroring when segment limit is reached

## Dependencies
- Functions called/Symbols referenced:
  - dsm_backend_startup (initialization)
  - dsm_create_descriptor (descriptor creation)
  - FreePageManagerGet/FreePageManagerPut (main region allocation)
  - make_main_region_dsm_handle (handle generation for main region)
  - dsm_impl_op (platform-specific segment operations)
  - pg_prng_uint32 (random handle generation)
  - ResourceOwnerForgetDSM (resource management)
- Called from (representative examples):
  - GetSessionDsmHandle (session management)
  - InitializeParallelDSM (parallel processing)
  - dsa_create_ext (dynamic shared arrays)
  - GetNamedDSMSegment (named segment registry)

## Notes and Other Information
- Must be called under postmaster or in single-user mode (safety assertion)
- Uses reference count of 2 initially (count of 1 triggers destruction)
- Integrates with CurrentResourceOwner for automatic cleanup
- Handle generation uses even numbers only for collision avoidance
- Supports both main shared memory region and system-level segment allocation
- Thread-safe through DynamicSharedMemoryControlLock usage
- Returns NULL only when DSM_CREATE_NULL_IF_MAXSEGMENTS flag is set and limit reached