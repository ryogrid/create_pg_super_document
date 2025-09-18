# ExecParallelHashTupleAlloc

## Location
[src/backend/executor/nodeHash.c:2956-3103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2956-L3103)

## Overview
Allocates space for hash join tuples in shared memory for parallel hash operations, handling dynamic growth of batches and buckets while coordinating between parallel workers.

## Definition


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
- : HashJoinTable containing parallel state and memory management structures
- : Size of memory to allocate for the tuple (automatically aligned to MAXALIGN boundary)
- : Output parameter receiving the DSA pointer to the allocated shared memory location

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN (memory alignment)
  - [dsa_get_address](../d/dsa_get_address.md) (convert DSA pointer to local address)
  - LWLockAcquire/LWLockRelease (exclusive locking)
  - dsa_allocate (shared memory allocation)
  - [ExecParallelHashIncreaseNumBatches](ExecParallelHashIncreaseNumBatches.md) (batch growth)
  - ExecParallelHashIncreaseNumBuckets (bucket growth)
  - BarrierPhase (parallel coordination)
  - HASH_CHUNK_DATA, HASH_CHUNK_HEADER_SIZE, HASH_CHUNK_THRESHOLD constants
- Called from:
  - [ExecParallelHashRepartitionFirst](ExecParallelHashRepartitionFirst.md) (nodeHash.c:1341)
  - ExecParallelHashTableInsert (nodeHash.c:1741)
  - ExecParallelHashTableInsertCurrentBatch (nodeHash.c:1800)

## Notes and Other Information
- This is a static function internal to nodeHash.c for parallel hash join operations
- Returns NULL when hash table structure changes, requiring caller to retry
- Implements sophisticated coordination between parallel workers using barriers and locks
- Manages both regular chunks (HASH_CHUNK_SIZE) and oversized chunks for large tuples
- Enforces space_allowed limits while ensuring each backend can allocate at least one chunk
- Uses DSA (Dynamic Shared Area) for cross-process memory management
- Fast path allocation avoids locking for optimal performance in common cases
- Growth decisions are based on NTUP_PER_BUCKET load factor limits and memory constraints