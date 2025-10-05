# ExecParallelHashTupleAlloc

## Location
[src/backend/executor/nodeHash.c:2956-3103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2956-L3103)

## Overview
Allocates space for hash join tuples in shared memory for parallel hash operations, handling dynamic growth of batches and buckets while coordinating between parallel workers.

## Definition

```c
static HashJoinTuple
ExecParallelHashTupleAlloc(HashJoinTable hashtable, size_t size,
						   dsa_pointer *shared)
```
## Detailed Description
 is the parallel equivalent of , designed specifically for parallel hash joins using shared memory. This function manages tuple allocation across multiple parallel workers while handling complex coordination scenarios including memory pressure, load factor management, and dynamic hash table growth.

The function implements a two-path allocation strategy:
1. **Fast path**: Allocates from the current backend's chunk without locking when sufficient space is available
2. **Slow path**: Acquires exclusive lock to allocate new chunks or handle growth conditions

Key coordination features include:
- **Growth handling**: Detects when bucket count or batch count needs to increase due to memory pressure or load factor limits
- **Space management**: Enforces per-backend memory limits while allowing at least one chunk per backend
- **Load factor monitoring**: Prevents hash table degradation by triggering bucket expansion when needed
- **Retry logic**: Returns NULL when structural changes require the caller to retry tuple placement

The function ensures thread-safe allocation while maintaining optimal performance through lock-free fast paths for common allocation scenarios.

## Parameters / Member Variables
- `hashtable`: HashJoinTable containing parallel state and memory management structures
- `size`: Size of memory to allocate for the tuple (automatically aligned to MAXALIGN boundary)
- `*shared`: Output parameter receiving the DSA pointer to the allocated shared memory location
## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN (memory alignment)
  - [dsa_get_address](../d/dsa_get_address.md) (convert DSA pointer to local address)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (exclusive locking)
  - dsa_allocate (shared memory allocation)
  - [ExecParallelHashIncreaseNumBatches](ExecParallelHashIncreaseNumBatches.md) (batch growth)
  - [ExecParallelHashIncreaseNumBuckets](ExecParallelHashIncreaseNumBuckets.md) (bucket growth)
  - [BarrierPhase](../B/BarrierPhase.md) (parallel coordination)
  - HASH_CHUNK_DATA, HASH_CHUNK_HEADER_SIZE, HASH_CHUNK_THRESHOLD constants
- Called from:
  - [ExecParallelHashRepartitionFirst](ExecParallelHashRepartitionFirst.md) (nodeHash.c:1341)
  - [ExecParallelHashTableInsert](ExecParallelHashTableInsert.md) (nodeHash.c:1741)
  - [ExecParallelHashTableInsertCurrentBatch](ExecParallelHashTableInsertCurrentBatch.md) (nodeHash.c:1800)

## Notes and Other Information
- This is a static function internal to nodeHash.c for parallel hash join operations
- Returns NULL when hash table structure changes, requiring caller to retry
- Implements sophisticated coordination between parallel workers using barriers and locks
- Manages both regular chunks (HASH_CHUNK_SIZE) and oversized chunks for large tuples
- Enforces space_allowed limits while ensuring each backend can allocate at least one chunk
- Uses DSA (Dynamic Shared Area) for cross-process memory management
- Fast path allocation avoids locking for optimal performance in common cases
- Growth decisions are based on NTUP_PER_BUCKET load factor limits and memory constraints

## Simplified Source

```c
static HashJoinTuple
ExecParallelHashTupleAlloc(HashJoinTable hashtable, size_t size,
                          dsa_pointer *shared)
{
    ParallelHashJoinState *pstate = hashtable->parallel_state;
    HashMemoryChunk chunk;
    Size chunk_size;
    HashJoinTuple result;

    size = MAXALIGN(size);

    // Fast path: try current chunk without locking
    chunk = hashtable->current_chunk;
    if (chunk != NULL && size <= HASH_CHUNK_THRESHOLD &&
        chunk->maxlen - chunk->used >= size)
    {
        *shared = hashtable->current_chunk_shared + HASH_CHUNK_HEADER_SIZE + chunk->used;
        result = (HashJoinTuple) (HASH_CHUNK_DATA(chunk) + chunk->used);
        chunk->used += size;
        return result;
    }

    // Slow path: acquire lock for new chunk allocation
    LWLockAcquire(&pstate->lock, LW_EXCLUSIVE);

    // Check if growth is needed (more batches or buckets)
    if (pstate->growth == PHJ_GROWTH_NEED_MORE_BATCHES ||
        pstate->growth == PHJ_GROWTH_NEED_MORE_BUCKETS)
    {
        ParallelHashGrowth growth = pstate->growth;
        hashtable->current_chunk = NULL;
        LWLockRelease(&pstate->lock);

        // Help with growth operation
        if (growth == PHJ_GROWTH_NEED_MORE_BATCHES)
            ExecParallelHashIncreaseNumBatches(hashtable);
        else if (growth == PHJ_GROWTH_NEED_MORE_BUCKETS)
            ExecParallelHashIncreaseNumBuckets(hashtable);

        return NULL; // Caller must retry
    }

    // Determine chunk size (oversized tuples get dedicated chunks)
    if (size > HASH_CHUNK_THRESHOLD)
        chunk_size = size + HASH_CHUNK_HEADER_SIZE;
    else
        chunk_size = HASH_CHUNK_SIZE;

    // Check memory and load factor limits for growth
    if (pstate->growth != PHJ_GROWTH_DISABLED)
    {
        // Check space limit
        if (hashtable->batches[0].at_least_one_chunk &&
            hashtable->batches[0].shared->size + chunk_size > pstate->space_allowed)
        {
            pstate->growth = PHJ_GROWTH_NEED_MORE_BATCHES;
            hashtable->batches[0].shared->space_exhausted = true;
            LWLockRelease(&pstate->lock);
            return NULL;
        }

        // Check load factor limit
        if (hashtable->nbatch == 1)
        {
            hashtable->batches[0].shared->ntuples += hashtable->batches[0].ntuples;
            hashtable->batches[0].ntuples = 0;
            if (hashtable->batches[0].shared->ntuples + 1 >
                hashtable->nbuckets * NTUP_PER_BUCKET &&
                hashtable->nbuckets < (INT_MAX / 2) &&
                hashtable->nbuckets * 2 <= MaxAllocSize / sizeof(dsa_pointer_atomic))
            {
                pstate->growth = PHJ_GROWTH_NEED_MORE_BUCKETS;
                LWLockRelease(&pstate->lock);
                return NULL;
            }
        }
    }

    // Allocate new chunk
    dsa_pointer chunk_shared = dsa_allocate(hashtable->area, chunk_size);
    hashtable->batches[hashtable->curbatch].shared->size += chunk_size;
    hashtable->batches[hashtable->curbatch].at_least_one_chunk = true;

    // Initialize chunk
    chunk = (HashMemoryChunk) dsa_get_address(hashtable->area, chunk_shared);
    *shared = chunk_shared + HASH_CHUNK_HEADER_SIZE;
    chunk->maxlen = chunk_size - HASH_CHUNK_HEADER_SIZE;
    chunk->used = size;

    // Add chunk to linked list
    chunk->next.shared = hashtable->batches[hashtable->curbatch].shared->chunks;
    hashtable->batches[hashtable->curbatch].shared->chunks = chunk_shared;

    // Set as current chunk for future fast-path allocations
    if (size <= HASH_CHUNK_THRESHOLD)
    {
        hashtable->current_chunk = chunk;
        hashtable->current_chunk_shared = chunk_shared;
    }

    LWLockRelease(&pstate->lock);
    result = (HashJoinTuple) HASH_CHUNK_DATA(chunk);
    return result;
}
```