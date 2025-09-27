# CreateAnonymousSegment

## Location
[src/backend/port/sysv_shmem.c:599-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L599-L674)

## Overview
Creates an anonymous memory-mapped shared memory segment with optional huge page support, handling fallback scenarios and size adjustments.

## Definition

```c
static void *
CreateAnonymousSegment(Size *size)
```
## Detailed Description
This function creates an anonymous shared memory segment using mmap() with support for huge pages. It first attempts to allocate using huge pages if configured (HUGE_PAGES_ON or HUGE_PAGES_TRY), rounding up the size to huge page boundaries to avoid kernel compatibility issues. If huge page allocation fails and the mode is HUGE_PAGES_TRY, it falls back to regular memory allocation. The function updates the  configuration to reflect the actual allocation method used.

The function modifies the input size parameter to reflect the actual allocated size, which may be larger than requested due to huge page alignment requirements. On allocation failure, it provides detailed error messages with hints about reducing memory usage through configuration parameters.

## Parameters / Member Variables
- : Pointer to requested size in bytes (modified to actual allocated size on success)

## Dependencies
- Functions called/Symbols referenced:
  - [GetHugePageSize](../G/GetHugePageSize.md)
  - [SetConfigOption](../S/SetConfigOption.md)
  - elog
  - ereport
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
- Constants referenced:
  - MAP_FAILED
  - HUGE_PAGES_ON
  - HUGE_PAGES_TRY
  - PG_MMAP_FLAGS
  - DEBUG1
  - PGC_INTERNAL
  - PGC_S_DYNAMIC_DEFAULT
- Called from (representative examples):
  - [PGSharedMemoryCreate](../P/PGSharedMemoryCreate.md)

## Notes and Other Information
- Static function only used within sysv_shmem.c
- Requires MAP_HUGETLB support for huge page functionality 
- Automatically rounds up allocation size to huge page boundaries when using huge pages
- Updates huge_pages_status configuration parameter to reflect actual allocation method
- Provides fallback from huge pages to regular pages when HUGE_PAGES_TRY is configured
- Returns MAP_FAILED on allocation failure with detailed error reporting
- Preserves errno from mmap() calls for accurate error reporting
- Provides helpful hints about reducing shared_buffers or max_connections on ENOMEM errors

## Simplified Source

```c
// Simplified version of CreateAnonymousSegment
static void *
CreateAnonymousSegment(Size *size)
{
    Size allocsize = *size;
    void *ptr = MAP_FAILED;

    // Step 1: Try huge pages if enabled
    if (huge_pages_enabled()) {
        // Round up size to huge page boundary
        Size hugepagesize = get_huge_page_size();
        allocsize = round_up_to_boundary(allocsize, hugepagesize);

        // Attempt huge page allocation
        ptr = mmap(NULL, allocsize, PROT_READ | PROT_WRITE,
                   PG_MMAP_FLAGS | MAP_HUGETLB, -1, 0);

        // Log failure if in try mode
        if (ptr == MAP_FAILED && huge_pages == HUGE_PAGES_TRY) {
            log_debug("huge pages allocation failed, falling back");
        }
    }

    // Step 2: Update configuration status
    SetConfigOption("huge_pages_status",
                    (ptr == MAP_FAILED) ? "off" : "on",
                    PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);

    // Step 3: Fallback to regular pages if needed
    if (ptr == MAP_FAILED && huge_pages != HUGE_PAGES_ON) {
        allocsize = *size;  // Use original size for fallback
        ptr = mmap(NULL, allocsize, PROT_READ | PROT_WRITE,
                   PG_MMAP_FLAGS, -1, 0);
    }

    // Step 4: Handle allocation failure
    if (ptr == MAP_FAILED) {
        ereport(FATAL,
                (errmsg("could not map anonymous shared memory"),
                 errhint("Reduce shared_buffers or max_connections")));
    }

    // Step 5: Return actual allocated size
    *size = allocsize;
    return ptr;
}
```

Key simplifications made:
- Abstracted platform-specific MAP_HUGETLB checks into conceptual functions
- Simplified huge page size calculation and rounding logic
- Consolidated error handling while preserving essential failure reporting
- Removed detailed errno preservation logic for clarity
- Focused on the main execution flow: try huge pages → fallback → error handling
- Maintained the core algorithm of size adjustment and memory allocation strategy