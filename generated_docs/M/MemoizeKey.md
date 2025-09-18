# MemoizeKey

## Location
src/backend/executor/nodeMemoize.c: 105 - 109

## Overview
MemoizeKey serves as the hash table key for cached entries in PostgreSQL's memoization system and includes LRU list management functionality.

## Definition


## Detailed Description
MemoizeKey is a crucial component of PostgreSQL's memoization cache that serves a dual purpose: it acts as the hash table key for cache lookups and simultaneously participates in the Least Recently Used (LRU) eviction strategy. The structure contains the parameter values that uniquely identify a cache entry and includes doubly-linked list nodes for efficient LRU management.

When the cache reaches capacity limits, the LRU mechanism uses the lru_node to identify and remove the least recently accessed entries. This design allows the memoization system to maintain optimal cache performance by keeping frequently accessed results while discarding older, less relevant entries.

## Parameters / Member Variables
- : A MinimalTuple containing the parameter values that serve as the unique identifier for cache lookups
- : A doubly-linked list node (dlist_node) used to maintain the LRU ordering of cache entries

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple
  - dlist_node
- Called from (representative examples):
  - MemoizeHash_hash
  - MemoizeHash_equal
  - prepare_probe_slot
  - remove_cache_entry
  - cache_reduce_memory
  - cache_lookup
  - cache_store_tuple

## Notes and Other Information
- Used as SH_KEY_TYPE in the specialized hash table implementation for memoization
- The params field must be properly initialized with parameter values for hash and equality comparisons
- LRU functionality is essential for cache management when memory limits are reached
- Hash and equality functions are provided specifically for this key type (MemoizeHash_hash, MemoizeHash_equal)