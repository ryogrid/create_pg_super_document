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
  - [dsm_cleanup_for_mmap](dsm_cleanup_for_mmap.md)
  - [dsm_control_bytes_needed](dsm_control_bytes_needed.md)
  - [pg_prng_uint32](../p/pg_prng_uint32.md)
  - [dsm_impl_op](dsm_impl_op.md)
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [dsm_postmaster_shutdown](dsm_postmaster_shutdown.md)
- Called from (representative examples):
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md)

## Notes and Other Information
- This function is called exactly once per cluster lifetime, only during postmaster startup
- Uses Assert(!IsUnderPostmaster) to ensure it's only called by the postmaster process
- The control segment handle is generated using a PRNG and restricted to even numbers
- The function loops until it finds an unused handle, avoiding DSM_HANDLE_INVALID
- Initializes global variables: dsm_control, dsm_control_handle, dsm_control_mapped_size
- Sets up automatic cleanup via on_shmem_exit() to ensure proper shutdown
- The control segment stores magic number, item count, and maximum items for validation
- For mmap implementations, performs cleanup of leftover segments from previous runs

## Simplified Source

```c
// Simplified version of dsm_postmaster_startup
void dsm_postmaster_startup(PGShmemHeader *shim) {
    void *dsm_control_address = NULL;
    uint32 maxitems;
    Size segsize;

    // Ensure this runs only in postmaster process
    Assert(!IsUnderPostmaster);

    // Step 1: Clean up any leftover mmap segments from previous runs
    if (dynamic_shared_memory_type == DSM_IMPL_MMAP) {
        dsm_cleanup_for_mmap();
    }

    // Step 2: Calculate control segment size based on max backends
    maxitems = PG_DYNSHMEM_FIXED_SLOTS + PG_DYNSHMEM_SLOTS_PER_BACKEND * MaxBackends;
    segsize = dsm_control_bytes_needed(maxitems);

    // Step 3: Find unused handle and create control segment
    for (;;) {
        // Generate random even-numbered handle
        dsm_control_handle = pg_prng_uint32(&pg_global_prng_state) << 1;
        if (dsm_control_handle == DSM_HANDLE_INVALID) {
            continue;  // Skip invalid handle
        }

        // Try to create segment with this handle
        if (dsm_impl_op(DSM_OP_CREATE, dsm_control_handle, segsize,
                       &dsm_control_impl_private, &dsm_control_address,
                       &dsm_control_mapped_size, ERROR)) {
            break;  // Success - handle is unique
        }
    }

    // Step 4: Initialize control segment and set up cleanup
    dsm_control = dsm_control_address;
    on_shmem_exit(dsm_postmaster_shutdown, PointerGetDatum(shim));
    shim->dsm_control = dsm_control_handle;

    // Step 5: Initialize control segment metadata
    dsm_control->magic = PG_DYNSHMEM_CONTROL_MAGIC;
    dsm_control->nitems = 0;
    dsm_control->maxitems = maxitems;
}
```

Key simplifications made:
- Removed debug logging statements for clarity
- Consolidated variable declarations at the top
- Added step-by-step comments explaining the main phases
- Simplified the loop logic explanation
- Focused on the core algorithm flow
- Abstracted low-level implementation details
- Preserved all essential initialization steps