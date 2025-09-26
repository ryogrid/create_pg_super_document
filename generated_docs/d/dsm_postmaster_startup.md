# dsm_postmaster_startup

## Location
[src/backend/storage/ipc/dsm.c:177-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L177-L237)

## Overview
Initializes the dynamic shared memory system during postmaster startup, creating and configuring the control segment that manages all DSM segments in the PostgreSQL cluster.

## Definition
```c
void dsm_postmaster_startup(PGShmemHeader *shim)
```

## Detailed Description
This function is responsible for the one-time initialization of PostgreSQL's dynamic shared memory system during postmaster startup. It creates a special control segment that serves as the registry for all DSM segments created during the cluster's lifetime. The function determines the appropriate size for the control segment based on the maximum number of backends, generates a unique handle for the control segment, initializes the segment's metadata, and sets up cleanup handlers. For mmap-based implementations, it also performs cleanup of any leftover segments from previous runs.

## Parameters / Member Variables
- `shim`: Pointer to the main shared memory header structure where the control segment handle will be stored

## Dependencies
- Functions called/Symbols referenced:
  - dsm_cleanup_for_mmap
  - dsm_control_bytes_needed
  - pg_prng_uint32
  - dsm_impl_op
  - on_shmem_exit
  - dsm_postmaster_shutdown
- Called from (representative examples):
  - CreateSharedMemoryAndSemaphores

## Notes and Other Information
- This function is called exactly once per cluster lifetime, only during postmaster startup
- Uses Assert(!IsUnderPostmaster) to ensure it's only called by the postmaster process
- The control segment handle is generated using a PRNG and restricted to even numbers
- The function loops until it finds an unused handle, avoiding DSM_HANDLE_INVALID
- Initializes global variables: dsm_control, dsm_control_handle, dsm_control_mapped_size
- Sets up automatic cleanup via on_shmem_exit() to ensure proper shutdown
- The control segment stores magic number, item count, and maximum items for validation
- For mmap implementations, performs cleanup of leftover segments from previous runs