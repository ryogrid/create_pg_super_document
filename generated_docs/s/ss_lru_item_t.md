# ss_lru_item_t

## Location
src/backend/access/common/syncscan.c: 97 - 102

## Overview
A doubly-linked list node structure that wraps scan location data in an LRU (Least Recently Used) cache for PostgreSQL's synchronized scan optimization system.

## Definition
```c
typedef struct ss_lru_item_t
{
    struct ss_lru_item_t *prev;
    struct ss_lru_item_t *next;
    ss_scan_location_t location;
} ss_lru_item_t;
```

## Detailed Description
The `ss_lru_item_t` structure implements a doubly-linked list node that contains scan location information for PostgreSQL's synchronized scan mechanism. This structure forms the backbone of an LRU cache that tracks the most recently used scan locations for different relations. The LRU design ensures that when the cache is full, the least recently accessed scan locations are evicted first, making room for new entries.

Each node contains pointers to the previous and next nodes in the linked list, along with the actual scan location data. This design allows for efficient insertion, deletion, and movement of nodes within the LRU cache while maintaining the scan location information needed for coordinating multiple sequential scans.

## Parameters / Member Variables
- `prev`: Pointer to the previous `ss_lru_item_t` node in the doubly-linked list, NULL if this is the first node
- `next`: Pointer to the next `ss_lru_item_t` node in the doubly-linked list, NULL if this is the last node  
- `location`: An `ss_scan_location_t` structure containing the actual scan location data (relation identifier and block position)

## Dependencies
- Functions called/Symbols referenced:
  - [ss_lru_item_t](ss_lru_item_t.md) (self-referential for prev/next pointers)
  - [ss_scan_location_t](ss_scan_location_t.md) (embedded member structure)
- Called from (representative examples):
  - [ss_scan_locations_t](ss_scan_locations_t.md) (used as array element)
  - SizeOfScanLocations (referenced for size calculation)
  - [SyncScanShmemInit](../S/SyncScanShmemInit.md) (used during shared memory initialization)
  - [ss_search](ss_search.md) (used in search operations)

## Notes and Other Information
- This structure is part of PostgreSQL's synchronized scan infrastructure in src/backend/access/common/syncscan.c
- The doubly-linked design allows for O(1) insertion and deletion operations in the LRU cache
- The structure is designed to work within fixed-size shared memory allocations
- Self-referential pointers enable efficient LRU cache operations like moving nodes to head/tail positions
- The embedded ss_scan_location_t structure contains the actual scan coordination data