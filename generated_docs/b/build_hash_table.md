# build_hash_table

## Location
[src/backend/executor/nodeMemoize.c:283-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L283-L301)

## Overview
Constructs a single hash table for a specific grouping set in the aggregation executor node, used for hash-based grouping operations.

## Definition
```c
static void build_hash_table(AggState *aggstate, int setno, long nbuckets)
```

## Detailed Description
This function builds a hash table for hash-based aggregation operations by calling BuildTupleHashTableExt() with appropriate parameters extracted from the AggState structure. It's used specifically for AGG_HASHED and AGG_MIXED aggregation strategies.

The function sets up the hash table with:
- Tuple descriptor from the hash slot
- Column information for grouping
- Hash and equality functions for key comparison
- Collation information for proper sorting/comparison
- Memory contexts for different allocation purposes
- Size calculations including space for transition data

The hash table is stored in the perhash structure corresponding to the given grouping set number.

## Parameters / Member Variables
- `aggstate`: Pointer to the aggregation state containing all necessary configuration and context information
- `setno`: Index of the grouping set for which to build the hash table (used to access perhash array)
- `nbuckets`: Number of hash buckets to allocate initially for the hash table

## Dependencies
- Functions called/Symbols referenced:
  - [AggState](../A/AggState.md)
  - [AggStatePerHash](../A/AggStatePerHash.md)
  - AGG_HASHED
  - AGG_MIXED
  - [AggStatePerGroupData](../A/AggStatePerGroupData.md)
  - [BuildTupleHashTableExt](../B/BuildTupleHashTableExt.md)
  - DO_AGGSPLIT_SKIPFINAL
- Called from (representative examples):
  - [build_hash_tables](build_hash_tables.md)
  - [ExecMemoize](../E/ExecMemoize.md)
  - [ExecInitRecursiveUnion](../E/ExecInitRecursiveUnion.md)
  - [ExecInitSetOp](../E/ExecInitSetOp.md)

## Notes and Other Information
- Only valid for AGG_HASHED and AGG_MIXED aggregation strategies (asserted at runtime)
- The additionalsize calculation accounts for transition data but excludes pass-by-reference values and representative tuples
- Uses three different memory contexts: metacxt for metadata, hashcxt for per-tuple data, and tmpcxt for temporary allocations
- The hash table size estimation helps ensure it doesn't exceed hash_mem configuration limits
- The DO_AGGSPLIT_SKIPFINAL parameter controls whether final aggregation functions should be skipped

## Simplified Source

```c
static void
build_hash_table(AggState *aggstate, int setno, long nbuckets)
{
    AggStatePerHash perhash = &aggstate->perhash[setno];
    MemoryContext metacxt = aggstate->hash_metacxt;
    MemoryContext hashcxt = aggstate->hashcontext->ecxt_per_tuple_memory;
    MemoryContext tmpcxt = aggstate->tmpcontext->ecxt_per_tuple_memory;
    Size additionalsize;

    // Ensure we're using hash-based aggregation
    Assert(aggstate->aggstrategy == AGG_HASHED || aggstate->aggstrategy == AGG_MIXED);

    // Calculate space needed for transition data
    additionalsize = aggstate->numtrans * sizeof(AggStatePerGroupData);

    // Build the hash table with all necessary parameters
    perhash->hashtable = BuildTupleHashTableExt(&aggstate->ss.ps,
                                               perhash->hashslot->tts_tupleDescriptor,
                                               perhash->numCols,
                                               perhash->hashGrpColIdxHash,
                                               perhash->eqfuncoids,
                                               perhash->hashfunctions,
                                               perhash->aggnode->grpCollations,
                                               nbuckets,
                                               additionalsize,
                                               metacxt,
                                               hashcxt,
                                               tmpcxt,
                                               DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit));
}
```