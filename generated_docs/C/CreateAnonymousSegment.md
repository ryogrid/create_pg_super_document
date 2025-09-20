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