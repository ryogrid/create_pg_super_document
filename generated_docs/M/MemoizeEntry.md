# MemoizeEntry

## Location
src/backend/executor/nodeMemoize.c: 115 - 123

## Overview
MemoizeEntry is the main data structure stored in PostgreSQL's memoization hash table, containing cached results and metadata for a specific set of input parameters.

## Definition


## Detailed Description
MemoizeEntry represents a complete cache entry in PostgreSQL's memoization system. It serves as the value type in the hash table, containing all necessary information about a cached result set for a specific combination of input parameters. The structure maintains a pointer to the hash key, a linked list of result tuples, cached hash value for performance, hash table status information, and a completion flag indicating whether the outer plan was fully processed.

The entry supports scenarios where multiple tuples may be returned for the same input parameters, using a linked list of MemoizeTuple structures. The completion flag is particularly important for determining whether additional tuples might be available from the outer plan, enabling proper cache behavior for partial result sets.

## Parameters / Member Variables
- : Pointer to the MemoizeKey containing parameter values and LRU list management
- : Pointer to the first MemoizeTuple in the linked list of cached results, or NULL if no tuples are cached
- hash: hash table empty: Cached hash value for efficient hash table operations without recalculation
- : Hash table status information (managed by the specialized hash table implementation)
- : Boolean flag indicating whether the outer plan has been read to completion for this parameter set

## Dependencies
- Functions called/Symbols referenced:
  - MemoizeKey
  - MemoizeTuple
- Called from (representative examples):
  - entry_purge_tuples
  - remove_cache_entry
  - cache_reduce_memory
  - cache_lookup
  - cache_store_tuple
  - ExecMemoize
  - ExecEndMemoize

## Notes and Other Information
- Used as SH_ELEMENT_TYPE in the specialized hash table implementation
- The complete flag prevents unnecessary re-execution of fully cached result sets
- Memory overhead calculations include this structure size in cache management decisions
- The hash value caching improves performance by avoiding recalculation during hash table operations
- Proper cleanup of the tuplehead linked list is essential during entry removal