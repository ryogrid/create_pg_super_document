# ExecChooseHashTableSize

## Location
[src/backend/executor/nodeHash.c:675-882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L675-L882)

## Overview
Determines optimal hash table parameters including bucket count, batch count, and memory allocation based on estimated tuple count, memory constraints, and performance considerations.

## Definition
void ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew, bool try_combined_hash_mem, int parallel_workers, size_t *space_allowed, int *numbuckets, int *numbatches, int *num_skew_mcvs)

## Detailed Description
ExecChooseHashTableSize is a sophisticated optimization function that calculates the most efficient hash table configuration for hash join operations. It balances multiple competing factors: memory usage, hash distribution efficiency, and execution performance. The function determines whether the hash table can fit entirely in memory (single batch) or requires spilling to disk (multiple batches).

The algorithm starts by estimating the total memory footprint of the inner relation, including tuple overhead and hash table structures. It then applies memory limits from PostgreSQL configuration (hash_mem GUC) and considers parallel execution scenarios where combined memory from multiple workers might be available.

For single-batch scenarios, it optimizes bucket count to achieve good hash distribution (targeting NTUP_PER_BUCKET tuples per bucket) while ensuring power-of-2 sizing for efficient hash computation. When memory is insufficient, it calculates the minimum number of batches needed and adjusts bucket count accordingly.

The function also incorporates skew optimization by estimating how many most-common-values (MCVs) can be handled in separate skew buckets, reducing the memory available for the main hash table but improving performance for skewed data distributions.

## Parameters / Member Variables
- ntuples: Estimated number of tuples in the inner relation
- tupwidth: Average width in bytes of tuples in the inner relation  
- useskew: Whether skew optimization should be considered
- try_combined_hash_mem: Whether to attempt using combined memory from all parallel workers
- parallel_workers: Number of parallel worker processes
- space_allowed: Output parameter for total memory allocation allowed
- numbuckets: Output parameter for optimal number of hash buckets (power of 2)
- numbatches: Output parameter for number of batches required (power of 2, minimum 1)
- num_skew_mcvs: Output parameter for number of most-common-values for skew optimization

## Dependencies
- Functions called/Symbols referenced:
  - [get_hash_memory_limit](../g/get_hash_memory_limit.md) (retrieves memory limit from GUC settings)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md) (rounds up to next power of 2)
  - pg_prevpower2_size_t (rounds down to previous power of 2)
  - [ExecChooseHashTableSize](ExecChooseHashTableSize.md) (recursive call for parallel fallback)
- Called from (representative examples):
  - [ExecHashTableCreate](ExecHashTableCreate.md) (hash table initialization)
  - [initial_cost_hashjoin](../i/initial_cost_hashjoin.md) (query planning cost estimation)

## Notes and Other Information
- Both bucket count and batch count must be powers of 2 for efficient hash computation using bitwise operations
- The function implements a fallback mechanism for parallel hash joins: first tries combined memory, then falls back to per-worker memory if batching is still required
- Skew optimization allocates up to SKEW_HASH_MEM_PERCENT of available memory for handling frequently occurring values
- Memory calculations include precise overhead estimates for hash tuple headers, bucket pointers, and skew bucket structures
- The algorithm ensures bucket arrays never exceed MaxAllocSize to prevent allocation failures
- For very small relations, enforces a minimum bucket count of 1024 to maintain reasonable hash distribution

## Simplified Source

```c
void
ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew,
                       bool try_combined_hash_mem, int parallel_workers,
                       size_t *space_allowed, int *numbuckets,
                       int *numbatches, int *num_skew_mcvs)
{
    int tupsize;
    size_t hash_table_bytes;
    int nbatch = 1;
    int nbuckets;

    // Force reasonable minimum if no estimate available
    if (ntuples <= 0.0)
        ntuples = 1000.0;

    // Calculate tuple size including hash table overhead
    tupsize = HJTUPLE_OVERHEAD + MAXALIGN(SizeofMinimalTupleHeader) + MAXALIGN(tupwidth);
    double inner_rel_bytes = ntuples * tupsize;

    // Get memory limit from configuration
    hash_table_bytes = get_hash_memory_limit();

    // For parallel hash, try combined memory from all workers
    if (try_combined_hash_mem)
    {
        double newlimit = (double) hash_table_bytes * (parallel_workers + 1);
        hash_table_bytes = (size_t) Min(newlimit, (double) SIZE_MAX);
    }

    *space_allowed = hash_table_bytes;

    // Handle skew optimization if requested
    if (useskew)
    {
        size_t bytes_per_mcv = tupsize + (8 * sizeof(HashSkewBucket *)) +
                              sizeof(int) + SKEW_BUCKET_OVERHEAD;
        size_t skew_mcvs = (hash_table_bytes / bytes_per_mcv * SKEW_HASH_MEM_PERCENT) / 100;

        *num_skew_mcvs = (int) Min(skew_mcvs, INT_MAX);

        // Reserve memory for skew buckets
        if (skew_mcvs > 0)
            hash_table_bytes -= skew_mcvs * bytes_per_mcv;
    }
    else
        *num_skew_mcvs = 0;

    // Calculate optimal bucket count
    size_t max_pointers = hash_table_bytes / sizeof(HashJoinTuple);
    max_pointers = Min(max_pointers, MaxAllocSize / sizeof(HashJoinTuple));
    max_pointers = pg_prevpower2_size_t(max_pointers);  // Must be power of 2

    double dbuckets = ceil(ntuples / NTUP_PER_BUCKET);
    nbuckets = (int) Min(dbuckets, max_pointers);
    nbuckets = Max(nbuckets, 1024);  // Enforce minimum
    nbuckets = pg_nextpower2_32(nbuckets);  // Round to power of 2

    // Check if we need multiple batches
    size_t bucket_bytes = sizeof(HashJoinTuple) * nbuckets;
    if (inner_rel_bytes + bucket_bytes > hash_table_bytes)
    {
        // Multiple batches needed
        if (try_combined_hash_mem)
        {
            // Fall back to per-worker memory and try again
            ExecChooseHashTableSize(ntuples, tupwidth, useskew, false,
                                  parallel_workers, space_allowed,
                                  numbuckets, numbatches, num_skew_mcvs);
            return;
        }

        // Recalculate buckets for batched execution
        size_t bucket_size = tupsize * NTUP_PER_BUCKET + sizeof(HashJoinTuple);
        size_t sbuckets = hash_table_bytes / bucket_size;
        nbuckets = pg_nextpower2_32((int) Min(sbuckets, max_pointers));
        bucket_bytes = nbuckets * sizeof(HashJoinTuple);

        // Calculate number of batches needed
        double dbatch = ceil(inner_rel_bytes / (hash_table_bytes - bucket_bytes));
        nbatch = pg_nextpower2_32(Max(2, (int) Min(dbatch, max_pointers)));
    }

    *numbuckets = nbuckets;
    *numbatches = nbatch;
}
```