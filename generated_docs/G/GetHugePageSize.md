# GetHugePageSize

## Location
[src/backend/port/sysv_shmem.c:479-577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L479-L577)

## Overview
Determines the system's huge page size and computes the appropriate mmap flags for huge page allocation, with platform-specific handling for Linux systems.

## Definition

```c
void
GetHugePageSize(Size *hugepagesize, int *mmap_flags)
```
## Detailed Description
This function identifies the huge page size to use and computes related mmap flags for shared memory allocation. It handles a Linux kernel bug where mmap() can fail on requests that aren't multiples of the hugepage size. The function rounds up requests to hugepage multiples to avoid compatibility issues and makes efficient use of the extra memory by increasing available space in the shmem header.

On Linux systems, it reads  to determine the default huge page size. If an explicit huge page size is configured via , it uses that value. Otherwise, it falls back to the system default or assumes 2MB if detection fails. The function also sets appropriate MAP_HUGETLB flags and includes explicit page size flags on recent Linux versions when necessary.

## Parameters / Member Variables
- : Output parameter to receive the determined huge page size in bytes (set to 0 if huge pages not supported)
- : Output parameter to receive the mmap flags for huge page allocation (set to 0 if huge pages not supported)

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md)
  - [FreeFile](../F/FreeFile.md)  
  - [pg_ceil_log2_64](../p/pg_ceil_log2_64.md)
- Called from (representative examples):
  - [CreateAnonymousSegment](../C/CreateAnonymousSegment.md)
  - [InitializeShmemGUCs](../I/InitializeShmemGUCs.md)

## Notes and Other Information
- Only functional on systems with MAP_HUGETLB support (primarily Linux)
- On non-supporting platforms, both output parameters are set to 0
- Reads from  on Linux to detect system huge page size
- Falls back to 2MB assumption if system detection fails
- Handles explicit page size specification via MAP_HUGE_MASK and MAP_HUGE_SHIFT on recent Linux versions
- The extra memory from rounding up to huge page boundaries is made available for additional locktable entries and other shared memory uses

## Simplified Source

```c
// Simplified version of GetHugePageSize
void GetHugePageSize(Size *hugepagesize, int *mmap_flags) {
#ifdef MAP_HUGETLB
    Size default_hugepagesize = 0;
    Size hugepagesize_local = 0;
    int mmap_flags_local = 0;

    // Step 1: Try to detect system default huge page size on Linux
#ifdef __linux__
    FILE *fp = AllocateFile("/proc/meminfo", "r");
    if (fp) {
        char buf[128];
        unsigned int sz;
        char ch;

        // Parse /proc/meminfo for "Hugepagesize: nnnn kB" line
        while (fgets(buf, sizeof(buf), fp)) {
            if (sscanf(buf, "Hugepagesize: %u %c", &sz, &ch) == 2) {
                if (ch == 'k') {
                    default_hugepagesize = sz * 1024;
                    break;
                }
            }
        }
        FreeFile(fp);
    }
#endif

    // Step 2: Determine which huge page size to use
    if (huge_page_size != 0) {
        // Use explicitly configured size
        hugepagesize_local = (Size) huge_page_size * 1024;
    }
    else if (default_hugepagesize != 0) {
        // Use system default if detected
        hugepagesize_local = default_hugepagesize;
    }
    else {
        // Fallback to 2MB assumption
        hugepagesize_local = 2 * 1024 * 1024;
    }

    // Step 3: Set up mmap flags for huge pages
    mmap_flags_local = MAP_HUGETLB;

    // Step 4: Add explicit page size flag if needed on recent Linux
#if defined(MAP_HUGE_MASK) && defined(MAP_HUGE_SHIFT)
    if (hugepagesize_local != default_hugepagesize) {
        int shift = pg_ceil_log2_64(hugepagesize_local);
        mmap_flags_local |= (shift & MAP_HUGE_MASK) << MAP_HUGE_SHIFT;
    }
#endif

    // Step 5: Return results to caller
    if (mmap_flags)
        *mmap_flags = mmap_flags_local;
    if (hugepagesize)
        *hugepagesize = hugepagesize_local;

#else
    // Platform doesn't support huge pages
    if (hugepagesize)
        *hugepagesize = 0;
    if (mmap_flags)
        *mmap_flags = 0;
#endif
}
```

Key simplifications made:
- Removed detailed comments and consolidated similar logic blocks
- Streamlined the Linux-specific /proc/meminfo parsing
- Clarified the decision logic for choosing huge page size
- Simplified the mmap flag construction
- Focused on the main execution path while preserving all essential functionality
- Added step-by-step comments to explain the core algorithm flow