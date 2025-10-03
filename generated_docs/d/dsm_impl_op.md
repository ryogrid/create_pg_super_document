# dsm_impl_op

## Location
[src/backend/storage/ipc/dsm_impl.c:159-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_impl.c#L159-L211)

## Overview
Platform-independent dispatcher function that routes dynamic shared memory operations to the appropriate platform-specific implementation based on the configured dynamic shared memory type.

## Definition

```c
bool
dsm_impl_op(dsm_op op, dsm_handle handle, Size request_size,
			void **impl_private, void **mapped_address, Size *mapped_size,
			int elevel)
```
## Detailed Description
The  function serves as a central dispatcher for all dynamic shared memory operations in PostgreSQL. It abstracts the platform-specific implementations by routing operations to the appropriate implementation (POSIX, System V, Windows, or memory-mapped files) based on the  configuration.

The function supports four fundamental operations:
- **DSM_OP_CREATE**: Create and map a new shared memory segment
- **DSM_OP_ATTACH**: Map an existing shared memory segment  
- **DSM_OP_DETACH**: Unmap a shared memory segment
- **DSM_OP_DESTROY**: Unmap and destroy a shared memory segment

This design allows PostgreSQL to support multiple shared memory implementations while providing a unified interface to callers.

## Parameters / Member Variables
- `op`: The operation to perform (CREATE/ATTACH/DETACH/DESTROY)
- `handle`: Handle of existing segment, or identifier for new segment in CREATE operations
- `request_size`: Requested size for CREATE operations, otherwise 0
- `**impl_private`: Pointer to implementation-specific private data, maintained across calls
- `**mapped_address`: Pointer to current mapping address, updated with new mapping
- `*mapped_size`: Pointer to current mapping size, updated with new size
- `elevel`: Error logging level for error messages
## Dependencies
- Functions called/Symbols referenced:
  - [dsm_impl_posix](dsm_impl_posix.md) (conditional on USE_DSM_POSIX)
  - [dsm_impl_sysv](dsm_impl_sysv.md) (conditional on USE_DSM_SYSV) 
  - [dsm_impl_windows](dsm_impl_windows.md) (conditional on USE_DSM_WINDOWS)
  - [dsm_impl_mmap](dsm_impl_mmap.md) (conditional on USE_DSM_MMAP)
- Called from (representative examples):
  - [dsm_create](dsm_create.md)
  - [dsm_attach](dsm_attach.md)
  - [dsm_detach](dsm_detach.md)
  - [dsm_backend_startup](dsm_backend_startup.md)
  - [dsm_postmaster_startup](dsm_postmaster_startup.md)

## Notes and Other Information
- Returns true on success, false on failure
- For DSM_OP_CREATE name collisions, should silently return false without logging
- Contains compile-time conditionals for different platform implementations
- Includes assertions to validate parameter consistency based on operation type
- The function acts as the single point of abstraction between the DSM API and platform-specific implementations

## Simplified Source

```c
// Simplified version of dsm_impl_op
bool dsm_impl_op(dsm_op op, dsm_handle handle, Size request_size,
                 void **impl_private, void **mapped_address, Size *mapped_size,
                 int elevel) {
    // Validate operation parameters
    Assert(op == DSM_OP_CREATE || request_size == 0);
    Assert((op != DSM_OP_CREATE && op != DSM_OP_ATTACH) ||
           (*mapped_address == NULL && *mapped_size == 0));

    // Route to platform-specific implementation based on configured type
    switch (dynamic_shared_memory_type) {
        case DSM_IMPL_POSIX:
            return dsm_impl_posix(op, handle, request_size, impl_private,
                                  mapped_address, mapped_size, elevel);

        case DSM_IMPL_SYSV:
            return dsm_impl_sysv(op, handle, request_size, impl_private,
                                 mapped_address, mapped_size, elevel);

        case DSM_IMPL_WINDOWS:
            return dsm_impl_windows(op, handle, request_size, impl_private,
                                    mapped_address, mapped_size, elevel);

        case DSM_IMPL_MMAP:
            return dsm_impl_mmap(op, handle, request_size, impl_private,
                                 mapped_address, mapped_size, elevel);

        default:
            // Unknown implementation type - error out
            elog(ERROR, "unexpected dynamic shared memory type: %d",
                 dynamic_shared_memory_type);
            return false;
    }
}
```

Key simplifications made:
- Removed platform-specific compile-time conditionals for clarity
- Consolidated the switch cases to show the core routing logic
- Added inline comments explaining the main steps
- Preserved essential parameter validation and error handling
- Focused on the dispatcher pattern that routes operations to implementations