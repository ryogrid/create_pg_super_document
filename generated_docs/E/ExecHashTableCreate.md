# ExecHashTableCreate

## Location
[src/backend/executor/nodeHash.c:432-671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L432-L671)

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
  - [ExecChooseHashTableSize](ExecChooseHashTableSize.md) (determines optimal hash table parameters)
  - palloc_object (allocates HashJoinTableData structure)
  - AllocSetContextCreate (creates memory contexts)
  - [get_op_hash_functions](../g/get_op_hash_functions.md) (retrieves hash function OIDs)
  - [fmgr_info](../f/fmgr_info.md) (initializes function manager info)
  - [ExecParallelHashJoinSetUpBatches](ExecParallelHashJoinSetUpBatches.md) (parallel coordination)
  - [ExecHashBuildSkewHash](ExecHashBuildSkewHash.md) (skew optimization setup)
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md) (main hash join execution)

## Notes and Other Information
- The hash table uses a power-of-2 number of buckets for efficient modulo operations using bitwise AND
- Memory is carefully managed through three contexts: hashCxt for long-lived data, batchCxt for current batch data, spillCxt for temporary spill operations  
- Parallel hash joins use shared memory and barriers for coordination among worker processes
- The function supports dynamic growth of batch count when initial memory estimates prove insufficient
- Skew optimization is only enabled for multi-batch joins where it can provide meaningful benefits
- File-based spilling is prepared but files are only opened when actually needed during execution

## Simplified Source

```c
HashJoinTable ExecHashTableCreate(HashState *state, List *hashOperators,
                                  List *hashCollations, bool keepNulls) {
    Hash *node;
    HashJoinTable hashtable;
    Plan *outerNode;
    size_t space_allowed;
    int nbuckets, nbatch, num_skew_mcvs, log2_nbuckets, nkeys, i;
    double rows;
    ListCell *ho, *hc;
    MemoryContext oldcxt;

    // Get size information for the relation to be hashed
    node = (Hash *) state->ps.plan;
    outerNode = outerPlan(node);
    rows = node->plan.parallel_aware ? node->rows_total : outerNode->plan_rows;

    // Determine optimal hash table size
    ExecChooseHashTableSize(rows, outerNode->plan_width,
                           OidIsValid(node->skewTable),
                           state->parallel_state != NULL,
                           state->parallel_state ? state->parallel_state->nparticipants - 1 : 0,
                           &space_allowed, &nbuckets, &nbatch, &num_skew_mcvs);

    // Ensure nbuckets is power of 2
    log2_nbuckets = my_log2(nbuckets);
    Assert(nbuckets == (1 << log2_nbuckets));

    // Initialize hash table control structure
    hashtable = palloc_object(HashJoinTableData);
    hashtable->nbuckets = nbuckets;
    hashtable->log2_nbuckets = log2_nbuckets;
    hashtable->keepNulls = keepNulls;
    hashtable->nbatch = nbatch;
    hashtable->curbatch = 0;
    hashtable->growEnabled = true;
    hashtable->totalTuples = 0;
    hashtable->spaceAllowed = space_allowed;
    hashtable->parallel_state = state->parallel_state;
    hashtable->buckets.unshared = NULL;

    // Initialize skew handling
    hashtable->skewEnabled = false;
    hashtable->skewBucket = NULL;
    hashtable->nSkewBuckets = 0;

    // Create memory contexts for different phases
    hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext,
                                              "HashTableContext",
                                              ALLOCSET_DEFAULT_SIZES);
    hashtable->batchCxt = AllocSetContextCreate(hashtable->hashCxt,
                                               "HashBatchContext",
                                               ALLOCSET_DEFAULT_SIZES);
    hashtable->spillCxt = AllocSetContextCreate(hashtable->hashCxt,
                                               "HashSpillContext",
                                               ALLOCSET_DEFAULT_SIZES);

    oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

    // Set up hash functions for each join key
    nkeys = list_length(hashOperators);
    hashtable->outer_hashfunctions = palloc_array(FmgrInfo, nkeys);
    hashtable->inner_hashfunctions = palloc_array(FmgrInfo, nkeys);
    hashtable->hashStrict = palloc_array(bool, nkeys);
    hashtable->collations = palloc_array(Oid, nkeys);

    i = 0;
    forboth(ho, hashOperators, hc, hashCollations) {
        Oid hashop = lfirst_oid(ho);
        Oid left_hashfn, right_hashfn;

        if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn)) {
            elog(ERROR, "could not find hash function for hash operator %u", hashop);
        }

        fmgr_info(left_hashfn, &hashtable->outer_hashfunctions[i]);
        fmgr_info(right_hashfn, &hashtable->inner_hashfunctions[i]);
        hashtable->hashStrict[i] = op_strict(hashop);
        hashtable->collations[i] = lfirst_oid(hc);
        i++;
    }

    // Set up batch file arrays for multi-batch joins (non-parallel)
    if (nbatch > 1 && hashtable->parallel_state == NULL) {
        MemoryContext oldctx = MemoryContextSwitchTo(hashtable->spillCxt);
        hashtable->innerBatchFile = palloc0_array(BufFile *, nbatch);
        hashtable->outerBatchFile = palloc0_array(BufFile *, nbatch);
        MemoryContextSwitchTo(oldctx);
        PrepareTempTablespaces();
    }

    MemoryContextSwitchTo(oldcxt);

    // Handle parallel vs non-parallel setup
    if (hashtable->parallel_state) {
        // Parallel setup: coordinate with other workers
        ParallelHashJoinState *pstate = hashtable->parallel_state;
        Barrier *build_barrier = &pstate->build_barrier;
        BarrierAttach(build_barrier);

        if (BarrierPhase(build_barrier) == PHJ_BUILD_ELECT &&
            BarrierArriveAndWait(build_barrier, WAIT_EVENT_HASH_BUILD_ELECT)) {
            // Elected to set up shared structures
            pstate->nbatch = nbatch;
            pstate->space_allowed = space_allowed;
            ExecParallelHashJoinSetUpBatches(hashtable, nbatch);
            pstate->nbuckets = nbuckets;
            ExecParallelHashTableAlloc(hashtable, 0);
        }
    } else {
        // Non-parallel setup: allocate bucket array
        MemoryContextSwitchTo(hashtable->batchCxt);
        hashtable->buckets.unshared = palloc0_array(HashJoinTuple, nbuckets);

        // Set up skew optimization for multi-batch joins
        if (nbatch > 1) {
            ExecHashBuildSkewHash(hashtable, node, num_skew_mcvs);
        }

        MemoryContextSwitchTo(oldcxt);
    }

    return hashtable;
}
```