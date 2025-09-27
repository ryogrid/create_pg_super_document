# SyncScanShmemSize

## Location
[src/backend/access/common/syncscan.c:126-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/syncscan.c#L126-L134)

## Overview
Calculates and reports the amount of shared memory space needed for PostgreSQL's synchronized scan feature.

## Definition
```c
Size SyncScanShmemSize(void)
```

## Detailed Description
SyncScanShmemSize is a utility function that computes the total shared memory required to support synchronized scanning functionality in PostgreSQL. It uses the SizeOfScanLocations macro to calculate the memory needed for the LRU cache that tracks scan locations across multiple concurrent table scans. The function is typically called during PostgreSQL's shared memory initialization phase to determine how much memory to allocate for the synchronized scan subsystem.

The synchronized scan feature allows multiple concurrent sequential scans of the same table to coordinate their positions, reducing overall I/O by having scans start near each other rather than all starting from the beginning of the table.

## Parameters / Member Variables
This function takes no parameters and returns a Size value representing the required shared memory in bytes.

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfScanLocations (macro)
  - SYNC_SCAN_NELEM (constant, value 20)
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md)

## Notes and Other Information
- The memory size calculation is based on SYNC_SCAN_NELEM (20), which defines the maximum number of large tables that can be tracked simultaneously in the LRU cache
- The function is part of PostgreSQL's shared memory subsystem initialization
- Used during server startup to determine total shared memory requirements
- The actual memory layout includes the ss_scan_locations_t structure plus space for SYNC_SCAN_NELEM ss_lru_item_t entries

## Simplified Source

```c
// Simplified version of SyncScanShmemSize
Size SyncScanShmemSize(void) {
    // Calculate memory needed for synchronized scan tracking
    // Returns size for LRU cache of scan positions (20 elements max)
    return SizeOfScanLocations(SYNC_SCAN_NELEM);
}
```

Key simplifications made:
- Added explanatory comments for the core purpose
- Clarified that it calculates memory for LRU cache of scan positions
- Noted the maximum element limit (SYNC_SCAN_NELEM = 20)
- Function is already quite simple, so main improvement is documentation clarity