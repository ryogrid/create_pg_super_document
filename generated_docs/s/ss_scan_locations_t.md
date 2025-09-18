# ss_scan_locations_t

## Location
src/backend/access/common/syncscan.c: 104 - 109

## Overview
The main data structure that manages the LRU cache of scan locations for PostgreSQL's synchronized scan optimization, containing head and tail pointers along with a fixed-size array of LRU items.

## Definition
```c
typedef struct ss_scan_locations_t
{
    ss_lru_item_t *head;
    ss_lru_item_t *tail;
    ss_lru_item_t items[FLEXIBLE_ARRAY_MEMBER]; /* SYNC_SCAN_NELEM items */
} ss_scan_locations_t;
```

## Detailed Description
The `ss_scan_locations_t` structure is the central management structure for PostgreSQL's synchronized scan LRU cache. It maintains an LRU (Least Recently Used) cache of scan locations to coordinate multiple sequential scans across different relations. The structure combines doubly-linked list management (via head and tail pointers) with a fixed-size array allocation for optimal memory usage in shared memory.

The structure is designed to hold a maximum of SYNC_SCAN_NELEM (20) scan location entries. When a new scan location needs to be cached and the cache is full, the least recently used item (at the tail) is evicted and replaced. The head pointer always points to the most recently accessed item, while the tail points to the least recently accessed item.

This design allows PostgreSQL to coordinate multiple sequential table scans by sharing information about where each scan is currently positioned, enabling new scans to potentially start from the current position of an ongoing scan rather than from the beginning of the table.

## Parameters / Member Variables
- `head`: Pointer to the most recently used `ss_lru_item_t` in the LRU cache, NULL if cache is empty
- `tail`: Pointer to the least recently used `ss_lru_item_t` in the LRU cache, NULL if cache is empty
- `items`: A flexible array member containing `ss_lru_item_t` structures, sized to hold SYNC_SCAN_NELEM (20) items

## Dependencies
- Functions called/Symbols referenced:
  - ss_lru_item_t (used for head, tail pointers and array elements)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member syntax)
  - SYNC_SCAN_NELEM (constant defining array size as 20)
- Called from (representative examples):
  - SizeOfScanLocations (used for size calculations)
  - SyncScanShmemInit (used during shared memory initialization)

## Notes and Other Information
- This structure is part of PostgreSQL's synchronized scan infrastructure in src/backend/access/common/syncscan.c
- The cache size is fixed at 20 items (SYNC_SCAN_NELEM), designed to handle the maximum number of large tables scanned simultaneously
- Uses flexible array member syntax for efficient memory allocation in shared memory
- The LRU design ensures optimal cache utilization by evicting least recently used scan locations first
- Head and tail pointers enable O(1) access to the most and least recently used items
- The structure assumes SYNC_SCAN_NELEM > 1 for proper LRU operation
- Larger cache sizes would mean more LRU list traversal overhead when starting new scans