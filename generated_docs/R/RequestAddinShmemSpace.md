# RequestAddinShmemSpace

## Location
[src/backend/storage/ipc/ipci.c:75-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipci.c#L75-L89)

## Overview
Allows loadable modules to request additional shared memory space during PostgreSQL initialization through the shmem_request_hook.

## Definition

```c
void
RequestAddinShmemSpace(Size size)
```
## Detailed Description
RequestAddinShmemSpace is a critical function that enables PostgreSQL extensions and loadable modules to request additional shared memory space beyond the core system requirements. This function can only be called during a specific phase of PostgreSQL startup - specifically from within the shmem_request_hook callback of libraries loaded via shared_preload_libraries. The function maintains a running total of all additional shared memory requests in the global variable total_addin_request, which is later used during shared memory allocation to ensure sufficient space is allocated for all extensions.

The function includes strict validation to ensure it's only called during the appropriate initialization phase, indicated by the process_shmem_requests_in_progress flag. Attempts to call this function outside of the designated hook will result in a FATAL error, terminating the postmaster process.

## Parameters / Member Variables
- `size`: The amount of additional shared memory space requested, specified as a Size type (typically size_t)
## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md) (utility function for safe size addition)
  - elog (error logging function)
- Called from (representative examples):
  - [test_slru_shmem_request](../t/test_slru_shmem_request.md) (from test modules)
  - Various extension shmem_request_hook implementations

## Notes and Other Information
- This function can only be called from within shmem_request_hook callbacks
- Calls from outside the designated initialization phase result in FATAL errors
- The function accumulates requests in the global total_addin_request variable
- Extensions must call this during shared_preload_libraries loading phase
- The requested memory is allocated as part of the main shared memory segment creation process