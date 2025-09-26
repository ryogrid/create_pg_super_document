# BuildTupleHashTableExt

## Location
src/backend/executor/execGrouping.c: 153 - 252

## Overview
Constructs an empty TupleHashTable with extended memory context control for hash-based grouping operations in PostgreSQL execution engine.

## Definition
```c
TupleHashTable BuildTupleHashTableExt(PlanState *parent,
                                     TupleDesc inputDesc,
                                     int numCols, AttrNumber *keyColIdx,
                                     const Oid *eqfuncoids,
                                     FmgrInfo *hashfunctions,
                                     Oid *collations,
                                     long nbuckets, Size additionalsize,
                                     MemoryContext metacxt,
                                     MemoryContext tablecxt,
                                     MemoryContext tempcxt, 
                                     bool use_variable_hash_iv);
```

## Detailed Description
This function creates a comprehensive hash table infrastructure for grouping tuples in PostgreSQL. It's the extended version that provides fine-grained memory context control, supporting separate contexts for metadata, table entries, and temporary evaluations. The function handles hash memory limits, parallel processing considerations, and JIT compilation settings. It sets up the table structure, initializes hash functions, creates tuple slots, builds equality comparison expressions, and prepares expression evaluation contexts. The function also handles variable hash initialization vectors for better distribution in parallel environments.

## Parameters / Member Variables
- `parent`: PlanState node that will own this hash table (affects JIT compilation)
- `inputDesc`: TupleDesc describing the structure of input tuples
- `numCols`: Number of columns to use as lookup keys
- `keyColIdx`: Array of column indices (AttrNumber) that serve as hash keys
- `eqfuncoids`: Array of equality function OIDs for key comparisons
- `hashfunctions`: Array of FmgrInfo structures for hashing functions
- `collations`: Array of collation OIDs for each key column
- `nbuckets`: Initial estimate of hash table size (limited by hash_mem)
- `additionalsize`: Size of additional data stored in each hash entry
- `metacxt`: Memory context for long-lived allocation (not per-entry data)
- `tablecxt`: Memory context for storing table entries
- `tempcxt`: Short-lived context for evaluating hash and comparison functions
- `use_variable_hash_iv`: Whether to use variable hash initialization vector for parallel processing

## Dependencies
- Functions called/Symbols referenced:
  - get_hash_memory_limit (determines memory limits for hash operations)
  - murmurhash32 (generates hash initialization vector for parallel workers)
  - CreateTupleDescCopy (creates copy of tuple descriptor)
  - MakeSingleTupleTableSlot (creates tuple slot with minimal tuple operations)
  - ExecBuildGroupingEqual (builds equality comparison expression)
  - CreateStandaloneExprContext (creates expression evaluation context)
  - tuplehash_create (creates underlying hash table structure)
- Called from (representative examples):
  - BuildTupleHashTable (simplified version that calls this extended version)
  - build_hash_table (in aggregate, recursive union, and set operation nodes)
  - buildSubPlanHash (for subplan hash operations)

## Notes and Other Information
- Automatically limits initial table size based on hash_mem setting to prevent excessive memory usage
- Supports parallel query execution with variable hash initialization vectors to improve load balancing
- Handles JIT compilation intelligently - disables JIT for old reset interface to prevent function lifetime issues
- Creates standalone expression context that relies on containing memory context for cleanup
- The tableslot is created lazily on first lookup operation
- All key-related arrays (keyColIdx, eqfuncoids, hashfunctions, collations) must live as long as the hash table
- Essential for hash-based aggregation, set operations, and subplan operations requiring efficient tuple grouping