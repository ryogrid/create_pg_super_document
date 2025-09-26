# ExecHashIncreaseNumBatches

## Location
[src/backend/executor/nodeHash.c:916-1079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L916-L1079)

## Overview
Dynamically doubles the number of batches in a hash table to reduce memory consumption by spilling approximately half of the current tuples to temporary files.

## Definition
static void ExecHashIncreaseNumBatches(HashJoinTable hashtable)

## Detailed Description
ExecHashIncreaseNumBatches implements PostgreSQL's dynamic memory management strategy for hash joins when memory consumption exceeds available limits. The function reorganizes the hash table by doubling the number of batches, which effectively redistributes tuples such that approximately half remain in memory while the other half are spilled to temporary batch files for later processing.

The function performs several critical operations: it doubles the batch count and enlarges or creates the batch file arrays if needed, scans through all existing hash table chunks to redistribute tuples based on their new batch assignments, keeps tuples belonging to the current batch in memory while spilling others to appropriate batch files, and optionally resizes the bucket array if an optimal size has been determined.

A key optimization is the ability to simultaneously resize the bucket array during rebatching, avoiding the need for a separate reorganization pass. The function also includes safeguards against pathological cases where all tuples have identical hash values, which would make further batching ineffective.

Memory management is carefully handled through proper context switching and chunk-by-chunk processing to avoid excessive memory usage during the reorganization process.

## Parameters / Member Variables
- hashtable: HashJoinTable structure containing the hash table state, batch configuration, memory chunks, and file arrays

## Dependencies
- Functions called/Symbols referenced:
  - palloc0_array/repalloc0_array (allocate/reallocate batch file arrays)
  - PrepareTempTablespaces (ensure temporary tablespace availability)
  - ExecHashGetBucketAndBatch (determine new batch assignment for tuples)
  - dense_alloc (allocate space in hash table chunks)
  - ExecHashJoinSaveTuple (save tuples to batch files)
  - CHECK_FOR_INTERRUPTS (allow query cancellation)
- Called from (representative examples):
  - ExecHashTableInsert (when memory limit exceeded during insertion)
  - ExecHashSkewTableInsert (when skew table memory limit exceeded)

## Dependencies
- Functions called/Symbols referenced:
  - palloc0_array/repalloc0_array (allocate/reallocate batch file arrays)
  - PrepareTempTablespaces (ensure temporary tablespace availability)
  - ExecHashGetBucketAndBatch (determine new batch assignment for tuples)
  - dense_alloc (allocate space in hash table chunks)
  - ExecHashJoinSaveTuple (save tuples to batch files)
  - CHECK_FOR_INTERRUPTS (allow query cancellation)
- Called from (representative examples):
  - ExecHashTableInsert (when memory limit exceeded during insertion)
  - ExecHashSkewTableInsert (when skew table memory limit exceeded)

## Notes and Other Information
- The function is static and only called internally when memory pressure is detected
- Batch count is always doubled to maintain power-of-2 sizing for efficient hash computation
- Growth can be permanently disabled if rebatching proves ineffective (all tuples have same hash value)
- File arrays are created lazily - first call allocates them, subsequent calls enlarge them
- The algorithm processes chunks directly rather than following bucket chains to ensure all tuples are handled exactly once
- Includes safety checks to prevent integer overflow in batch count calculations
- Bucket array resizing is performed opportunistically during rebatching for efficiency