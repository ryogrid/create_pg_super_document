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

## Simplified Source

```c
// Simplified version of dsm_shmem_init
void dsm_shmem_init(void) {
    // Step 1: Calculate required space for dynamic shared memory
    size_t size = dsm_estimate_size();
    bool found;

    // Step 2: Early return if no dynamic shared memory needed
    if (size == 0)
        return;

    // Step 3: Allocate space in main shared memory segment
    dsm_main_space_begin = ShmemInitStruct("Preallocated DSM", size, &found);

    // Step 4: Initialize free page manager (only for new allocation)
    if (!found) {
        FreePageManager *fpm = (FreePageManager *) dsm_main_space_begin;
        size_t first_page = 0;
        size_t pages;

        // Reserve space for the FreePageManager control structure
        while (first_page * FPM_PAGE_SIZE < sizeof(FreePageManager))
            ++first_page;

        // Initialize the page manager and give it the remaining space
        FreePageManagerInitialize(fpm, dsm_main_space_begin);
        pages = (size / FPM_PAGE_SIZE) - first_page;
        FreePageManagerPut(fpm, first_page, pages);
    }
}
```

Key simplifications made:
- Added step-by-step comments explaining the main logic flow
- Preserved the essential algorithm and error-free execution path
- Maintained all critical functionality including size calculation, space allocation, and page manager initialization
- Kept the early return optimization for zero-size case
- Focused on the main execution path while preserving correctness