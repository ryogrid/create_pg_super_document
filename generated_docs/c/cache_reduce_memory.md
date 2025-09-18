# cache_reduce_memory

## Location
src/backend/executor/nodeMemoize.c: 440 - 527

## Overview
Evicts cache entries using an LRU (Least Recently Used) strategy to reduce memory consumption below the configured limit, with special handling to protect entries currently being populated.

## Definition
```c
static bool cache_reduce_memory(MemoizeState *mstate, MemoizeKey *specialkey)
```

## Detailed Description
This function implements the cache eviction mechanism for the PostgreSQL memoize node. When memory usage exceeds the configured limit, it systematically removes the least recently used cache entries until memory consumption drops back within acceptable bounds. The function uses the LRU list to determine eviction order and includes sophisticated logic to handle the case where cache eviction might occur while new entries are being populated.

The function returns a boolean indicating whether a 'special' cache entry (if specified) remains intact after the eviction process. This is crucial for maintaining consistency when eviction occurs during cache population operations.

## Parameters / Member Variables
- `mstate`: Pointer to the MemoizeState structure containing cache state and memory tracking information
- `specialkey`: Optional pointer to a MemoizeKey that should be protected during eviction; if this key's entry gets evicted, the function returns false

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach_modify (iterates through LRU list with modification capability)
  - dlist_container (retrieves key from LRU list node)
  - prepare_probe_slot (sets up hash table lookup)
  - memoize_lookup (finds cache entry in hash table)
  - remove_cache_entry (removes and frees cache entry)
  - Assert (debugging assertion)
  - elog (error logging)
- Types referenced:
  - MemoizeState
  - MemoizeKey
  - MemoizeEntry
  - dlist_mutable_iter
- Called from:
  - cache_lookup
  - cache_store_tuple

## Notes and Other Information
- This is a static function, only accessible within nodeMemoize.c
- The function updates peak memory usage statistics before beginning eviction
- Uses LRU (Least Recently Used) eviction policy, starting from the head of the LRU list
- Includes sophisticated error checking to detect hash table corruption or misbehaving hash functions
- The specialkey parameter allows callers to detect if their current working entry was evicted
- LRU pointers are stored in the key rather than the entry due to hash table resizing considerations
- Memory limit enforcement is strict - eviction continues until memory usage is at or below the limit
- Statistics tracking includes both eviction counts and peak memory usage
- Hash table lookup is required for each LRU entry due to the architectural decision to store LRU pointers in keys rather than entries