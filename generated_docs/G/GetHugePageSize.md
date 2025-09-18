# GetHugePageSize

## Location
src/backend/port/sysv_shmem.c: 479 - 577

## Overview
Determines the system's huge page size and computes the appropriate mmap flags for huge page allocation, with platform-specific handling for Linux systems.

## Definition


## Detailed Description
This function identifies the huge page size to use and computes related mmap flags for shared memory allocation. It handles a Linux kernel bug where mmap() can fail on requests that aren't multiples of the hugepage size. The function rounds up requests to hugepage multiples to avoid compatibility issues and makes efficient use of the extra memory by increasing available space in the shmem header.

On Linux systems, it reads  to determine the default huge page size. If an explicit huge page size is configured via , it uses that value. Otherwise, it falls back to the system default or assumes 2MB if detection fails. The function also sets appropriate MAP_HUGETLB flags and includes explicit page size flags on recent Linux versions when necessary.

## Parameters / Member Variables
- : Output parameter to receive the determined huge page size in bytes (set to 0 if huge pages not supported)
- : Output parameter to receive the mmap flags for huge page allocation (set to 0 if huge pages not supported)

## Dependencies
- Functions called/Symbols referenced:
  - AllocateFile
  - FreeFile  
  - pg_ceil_log2_64
- Called from (representative examples):
  - CreateAnonymousSegment
  - InitializeShmemGUCs

## Notes and Other Information
- Only functional on systems with MAP_HUGETLB support (primarily Linux)
- On non-supporting platforms, both output parameters are set to 0
- Reads from  on Linux to detect system huge page size
- Falls back to 2MB assumption if system detection fails
- Handles explicit page size specification via MAP_HUGE_MASK and MAP_HUGE_SHIFT on recent Linux versions
- The extra memory from rounding up to huge page boundaries is made available for additional locktable entries and other shared memory uses