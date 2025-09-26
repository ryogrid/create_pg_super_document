# dsm_shmem_init

## Location
[src/backend/storage/ipc/dsm.c:479-515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L479-L515)

## Overview
Initializes the dynamic shared memory management space within the main shared memory segment using a FreePageManager for allocation tracking.

## Definition
```c
void dsm_shmem_init(void)
```

## Detailed Description
This function sets up the dynamic shared memory management infrastructure during PostgreSQL startup. It allocates a reserved space within the main shared memory segment that will be used for managing dynamic shared memory operations. The function uses a FreePageManager to track and manage the allocated space efficiently.

The function first determines the required size by calling dsm_estimate_size(), which calculates space based on the min_dynamic_shared_memory configuration parameter. If no space is needed (size == 0), the function returns early.

For new installations, the function initializes a FreePageManager structure at the beginning of the allocated space and gives it control over the remaining space divided into fixed-size pages. The FreePageManager handles subsequent allocation and deallocation of space within this reserved area.

In cases where the shared memory structure already exists (found == true), the function simply establishes the pointer to the existing space without reinitializing it.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_estimate_size](dsm_estimate_size.md)
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [FreePageManagerInitialize](../F/FreePageManagerInitialize.md)
  - [FreePageManagerPut](../F/FreePageManagerPut.md)
  - [FreePageManager](../F/FreePageManager.md) (type)
  - FPM_PAGE_SIZE
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)

## Notes and Other Information
- Only performs initialization when size > 0 (when dynamic shared memory is configured)
- Uses page-based allocation through FreePageManager for efficient space management
- Reserves space at the beginning for the FreePageManager control structure itself
- Calculates first usable page based on FreePageManager structure size and page alignment
- Part of PostgreSQL's shared memory initialization sequence during startup
- Sets global dsm_main_space_begin pointer for use by other DSM functions
- The FreePageManager provides bitmap-based tracking of allocated vs free pages
- Works in conjunction with dsm_estimate_size() to properly size the allocation