# _hash_getcachedmetap

## Location
src/backend/access/hash/hashpage.c: 1501 - 1558

## Overview
Returns cached metapage data for a hash index, refreshing the cache if necessary or if explicitly requested.

## Definition


## Detailed Description
This function provides access to cached hash index metapage data, implementing a caching mechanism to avoid repeatedly reading the metapage from disk. The function:

1. Checks if the cache needs to be refreshed (either because it's uninitialized or force_refresh is true)
2. If refresh is needed, allocates memory for the cache in the relation's index context
3. Reads the metapage either from the provided buffer or by fetching it from disk
4. Copies the metapage data into the cache
5. Returns a pointer to the cached metapage data

The caching is done at the relation level (rel->rd_amcache) and persists across multiple function calls. The function carefully handles memory allocation and buffer management to avoid setting invalid cache values.

## Parameters / Member Variables
- : The hash index relation whose metapage data is being cached
- : Pointer to buffer containing the metapage (may be InvalidBuffer initially)
- : Boolean flag to force cache refresh even if cache is valid

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAlloc (allocates memory for cache in relation's context)
  - _hash_getbuf (reads metapage from disk if buffer not provided)
  - HashPageGetMeta (extracts metapage data from page)
  - BufferGetPage (gets page from buffer)
  - LockBuffer (acquires and releases buffer locks)
  - memcpy (copies metapage data into cache)
  - HASH_METAPAGE (constant for metapage block number)
  - HASH_READ (read access mode)
  - LH_META_PAGE (lock mode for metapage)
- Called from (representative examples):
  - hashbulkdelete (during bulk delete operations)
  - _hash_getbucketbuf_from_hashkey (when accessing bucket information)

## Notes and Other Information
- The cache is allocated in the relation's index context (rel->rd_indexcxt) ensuring it persists with the relation
- The function maintains a pin on the metapage buffer but releases any locks before returning
- Memory allocation and buffer reading are done carefully to avoid setting rd_amcache to invalid values
- If a metabuf is provided, the caller must hold a pin but no lock on it
- The function is thread-safe as long as callers properly manage buffer pins and locks
- This caching mechanism significantly improves performance for hash index operations that frequently need metapage information
- The cache is automatically invalidated when the relation cache entry is invalidated