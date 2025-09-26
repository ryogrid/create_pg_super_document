# ExecHashTableCreate

## Location
src/backend/executor/nodeHash.c: 432 - 671

## Overview
Creates and initializes an empty hash table data structure for hash join operations, configuring memory contexts, hash functions, and batch processing parameters.

## Definition
HashJoinTable ExecHashTableCreate(HashState *state, List *hashOperators, List *hashCollations, bool keepNulls)

## Detailed Description
ExecHashTableCreate is a comprehensive function that constructs the core hash table infrastructure used in PostgreSQL hash join operations. It performs several critical tasks: determining optimal hash table sizing based on estimated row counts and available memory, initializing the HashJoinTable control structure with appropriate parameters, setting up memory contexts for different phases of hash join execution, configuring hash functions for each join key, and preparing for both single-batch and multi-batch processing scenarios.

The function supports both regular and parallel hash joins, with special handling for shared hash tables in parallel query execution. It implements sophisticated memory management through multiple memory contexts (hashCxt, batchCxt, spillCxt) to properly isolate different types of allocations. For multi-batch scenarios, it sets up file arrays for spilling data to disk when memory is insufficient.

The function also includes optimizations such as skew handling for frequently occurring values and dynamic batch size adjustment capabilities through the growEnabled flag.

## Parameters / Member Variables
- state: HashState containing execution state and parallel coordination information
- hashOperators: List of hash operators (one per join key) for computing hash values  
- hashCollations: List of collation OIDs corresponding to each hash operator
- keepNulls: Boolean flag indicating whether NULL values should be preserved in the hash table

## Dependencies
- Functions called/Symbols referenced:
  - ExecChooseHashTableSize (determines optimal hash table parameters)
  - palloc_object (allocates HashJoinTableData structure)
  - AllocSetContextCreate (creates memory contexts)
  - get_op_hash_functions (retrieves hash function OIDs)
  - fmgr_info (initializes function manager info)
  - ExecParallelHashJoinSetUpBatches (parallel coordination)
  - ExecHashBuildSkewHash (skew optimization setup)
- Called from (representative examples):
  - ExecHashJoinImpl (main hash join execution)

## Notes and Other Information
- The hash table uses a power-of-2 number of buckets for efficient modulo operations using bitwise AND
- Memory is carefully managed through three contexts: hashCxt for long-lived data, batchCxt for current batch data, spillCxt for temporary spill operations  
- Parallel hash joins use shared memory and barriers for coordination among worker processes
- The function supports dynamic growth of batch count when initial memory estimates prove insufficient
- Skew optimization is only enabled for multi-batch joins where it can provide meaningful benefits
- File-based spilling is prepared but files are only opened when actually needed during execution