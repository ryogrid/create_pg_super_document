# SyncScanShmemInit

## Location
[src/backend/access/common/syncscan.c:135-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/syncscan.c#L135-L190)

## Overview
Initializes the shared memory structures needed for PostgreSQL's synchronized scan feature.

## Definition
```c
void SyncScanShmemInit(void)
```

## Detailed Description
SyncScanShmemInit is responsible for setting up the shared memory infrastructure required for synchronized scanning functionality. This function is called during PostgreSQL's shared memory initialization phase and performs two main tasks:

1. **Memory Allocation**: Uses ShmemInitStruct to allocate or attach to shared memory for the scan locations list
2. **Structure Initialization**: If this is the postmaster process (!IsUnderPostmaster), it initializes the LRU (Least Recently Used) cache structure with invalid values

The function creates a doubly-linked list structure where each item represents a scan location for a table. All entries are initially marked as invalid and will be replaced with real scan locations as tables are accessed. The LRU design ensures that the most recently used scan locations are kept available while older, unused entries are evicted.

## Parameters / Member Variables
This function takes no parameters and returns void.

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](ShmemInitStruct.md)
  - SizeOfScanLocations (macro)
  - SYNC_SCAN_NELEM (constant)
  - [ss_scan_locations_t](../s/ss_scan_locations_t.md) (type)
  - [ss_lru_item_t](../s/ss_lru_item_t.md) (type)
  - InvalidOid
  - InvalidRelFileNumber
  - InvalidBlockNumber
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)

## Notes and Other Information
- Only the postmaster process initializes the data structures; worker processes just attach to existing shared memory
- All LRU list items are pre-linked in a doubly-linked list for efficient insertion and removal operations
- Invalid values are used to mark unused slots: InvalidOid, InvalidRelFileNumber, InvalidBlockNumber
- The function sets up head and tail pointers for the LRU list to enable O(1) operations
- Critical for multi-process coordination of table scan starting positions
- Part of PostgreSQL's shared memory subsystem initialization sequence

## Simplified Source

```c
// Simplified version of SyncScanShmemInit
void SyncScanShmemInit(void) {
    bool found;

    // Step 1: Allocate or attach to shared memory for scan locations
    scan_locations = (ss_scan_locations_t *)
        ShmemInitStruct("Sync Scan Locations List",
                       SizeOfScanLocations(SYNC_SCAN_NELEM),
                       &found);

    // Step 2: Initialize structures only in postmaster process
    if (!IsUnderPostmaster) {
        // Set up LRU list head and tail pointers
        scan_locations->head = &scan_locations->items[0];
        scan_locations->tail = &scan_locations->items[SYNC_SCAN_NELEM - 1];

        // Step 3: Initialize all LRU cache entries with invalid values
        for (int i = 0; i < SYNC_SCAN_NELEM; i++) {
            ss_lru_item_t *item = &scan_locations->items[i];

            // Mark entry as invalid (will be replaced by real scan locations)
            item->location.relfilelocator.spcOid = InvalidOid;
            item->location.relfilelocator.dbOid = InvalidOid;
            item->location.relfilelocator.relNumber = InvalidRelFileNumber;
            item->location.location = InvalidBlockNumber;

            // Link items in doubly-linked list
            item->prev = (i > 0) ? &scan_locations->items[i - 1] : NULL;
            item->next = (i < SYNC_SCAN_NELEM - 1) ? &scan_locations->items[i + 1] : NULL;
        }
    }
    // Worker processes just verify shared memory was found
}
```

Key simplifications made:
- Removed detailed comments that explained the obvious
- Consolidated variable declarations inline where appropriate
- Simplified the conditional logic for prev/next pointer assignment
- Added high-level step comments to show the main phases
- Focused on the core algorithm: allocate memory, set boundaries, initialize LRU list
- Preserved the essential dual-role behavior (postmaster vs worker process)