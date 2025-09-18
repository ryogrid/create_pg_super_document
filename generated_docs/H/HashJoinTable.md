# HashJoinTable

## Location
src/include/nodes/execnodes.h: 2187 - 2188

## Overview
HashJoinTable is a typedef for a pointer to HashJoinTableData structure that represents the in-memory hash table used during hash join operations in PostgreSQL.

## Definition
```c
typedef struct HashJoinTableData *HashJoinTable;
```

## Detailed Description
HashJoinTable serves as the main data structure for hash join execution in PostgreSQL. It encapsulates all the state and metadata required for building and probing hash tables during join operations. The structure supports both regular and parallel hash joins, with features for dynamic resizing, batch processing for large datasets that dont fit in memory, skew optimization for handling data distribution issues, and efficient memory management through multiple memory contexts.

The hash table can operate in multiple modes:
- Single-batch mode for datasets that fit entirely in memory
- Multi-batch mode where data is partitioned across multiple temporary files
- Parallel mode where multiple workers cooperate to build and probe the hash table
- Skew optimization mode to handle highly skewed data distributions

## Parameters / Member Variables
- `nbuckets`: Number of buckets in the current in-memory hash table
- `log2_nbuckets`: Log base 2 of nbuckets (nbuckets must be power of 2)
- `nbuckets_original`: Original number of buckets when starting first hash
- `nbuckets_optimal`: Optimal number of buckets per batch
- `log2_nbuckets_optimal`: Log base 2 of optimal bucket count
- `buckets`: Union containing either unshared array (per-batch) or shared array (per-query DSA) of tuple pointers
- `keepNulls`: Flag indicating whether to store unmatchable NULL tuples
- `skewEnabled`: Flag indicating if skew optimization is active
- `skewBucket`: Hash table of skew buckets for handling data skew
- `skewBucketLen`: Size of skew bucket array (power of 2)
- `nSkewBuckets`: Number of currently active skew buckets
- `skewBucketNums`: Array indexes of active skew buckets
- `nbatch`: Total number of batches for processing
- `curbatch`: Current batch number (0 during first pass)
- `nbatch_original`: Original batch count when inner scan started
- `nbatch_outstart`: Batch count when outer scan started
- `growEnabled`: Flag to control whether batch count can be increased
- `totalTuples`: Total number of tuples from inner plan
- `partialTuples`: Number of tuples obtained by this backend
- `skewTuples`: Number of tuples inserted into skew buckets
- `innerBatchFile`: Array of temp files for inner relation batches
- `outerBatchFile`: Array of temp files for outer relation batches
- `outer_hashfunctions`: Hash function lookup data for outer relation
- `inner_hashfunctions`: Hash function lookup data for inner relation
- `hashStrict`: Array indicating if each hash operator is strict
- `collations`: Collation OIDs for hash operations
- `spaceUsed`: Current memory usage by tuples
- `spaceAllowed`: Memory usage upper limit
- `spacePeak`: Peak memory usage recorded
- `spaceUsedSkew`: Current skew hash table memory usage
- `spaceAllowedSkew`: Skew hash table memory limit
- `hashCxt`: Memory context for entire hash join lifetime
- `batchCxt`: Memory context for current batch only
- `spillCxt`: Memory context for spilling to temp files
- `chunks`: Memory chunk list for dense tuple allocation
- `current_chunk`: Current memory chunk for this backend
- `area`: DSA area for parallel hash join memory allocation
- `parallel_state`: Shared state for parallel hash joins
- `batches`: Batch accessors for parallel processing
- `current_chunk_shared`: Shared pointer to current chunk

## Dependencies
- Functions called/Symbols referenced:
  - HashJoinTableData (underlying struct definition)
  - HashJoinTupleData (tuple storage format)
  - HashSkewBucket (skew optimization buckets)
  - BufFile (temporary file management)
  - FmgrInfo (function manager info)
  - MemoryContext (memory management)
  - ParallelHashJoinState (parallel execution state)
- Called from (representative examples):
  - ExecHashTableCreate
  - ExecHashTableDestroy
  - ExecHashTableInsert
  - ExecScanHashBucket
  - ExecHashJoinImpl

## Notes and Other Information
- The hash table automatically resizes both bucket count and batch count based on memory pressure and data volume
- Skew optimization creates separate hash buckets for frequently occurring values to prevent hash bucket overflow
- Parallel hash joins use DSA (Dynamic Shared Area) for shared memory allocation across worker processes
- The structure supports graceful degradation from single-batch to multi-batch processing when memory is exhausted
- Memory contexts are carefully organized to allow efficient cleanup of different phases of hash join execution