# ss_search

## Location
[src/backend/access/common/syncscan.c:191-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/syncscan.c#L191-L253)

## Overview
Searches the synchronized scan locations LRU cache for an entry matching a given relfilelocator and optionally updates its location.

## Definition
```c
static BlockNumber ss_search(RelFileLocator relfilelocator, BlockNumber location, bool set)
```

## Detailed Description
ss_search is a core function of PostgreSQL's synchronized scan implementation that manages the LRU (Least Recently Used) cache of scan locations. The function performs several key operations:

1. **Search**: Traverses the LRU list to find an entry matching the provided RelFileLocator
2. **Update**: If found and `set` is true, updates the stored location to the new value
3. **Create**: If no match is found, takes over the least recently used entry (tail) and initializes it with the new relfilelocator and location
4. **LRU Management**: Moves the accessed/created entry to the head of the LRU list to mark it as most recently used

The function implements an efficient LRU cache where frequently accessed tables remain available for synchronized scanning while infrequently used entries are evicted. The doubly-linked list structure allows for O(1) insertion and removal operations.

## Parameters / Member Variables
- `relfilelocator`: A RelFileLocator structure identifying the specific table/relation file
- `location`: The BlockNumber representing the scan position to set (if `set` is true) or the initial position for new entries
- `set`: Boolean flag indicating whether to update the location of an existing entry

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocatorEquals (comparison function)
  - [ss_lru_item_t](ss_lru_item_t.md) (data structure type)
- Called from (representative examples):
  - [ss_get_location](ss_get_location.md)
  - [ss_report_location](ss_report_location.md)

## Notes and Other Information
- This is a static function, only accessible within the syncscan.c file
- Caller must hold appropriate locks on the shared data structure before calling
- The function always returns the current location for the relfilelocator after any updates
- LRU eviction happens automatically when the cache is full and a new entry is needed
- The search is linear through the LRU list, which is acceptable given the relatively small SYNC_SCAN_NELEM size (20)
- Critical for coordinating scan starting positions between multiple concurrent scans of the same table