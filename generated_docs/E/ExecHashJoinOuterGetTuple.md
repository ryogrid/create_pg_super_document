# ExecHashJoinOuterGetTuple

## Location
[src/backend/executor/nodeHashjoin.c:890-963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L890-L963)

## Overview
ExecHashJoinOuterGetTuple retrieves the next outer tuple for a non-parallel hash join, handling both initial tuple fetching from the outer plan node and subsequent tuple reading from batch files.

## Definition

```c
static TupleTableSlot *
ExecHashJoinOuterGetTuple(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue)
```
## Detailed Description
ExecHashJoinOuterGetTuple is responsible for supplying outer tuples to the hash join algorithm in non-parallel execution. It handles two distinct scenarios:

1. **First Pass (Batch 0)**: During the initial execution, it fetches tuples directly from the outer plan node using ExecProcNode(). It checks for a pre-fetched tuple that might have been stored during the empty-outer optimization check, and computes hash values for valid tuples.

2. **Subsequent Batches**: For multi-batch hash joins, it reads previously saved tuples from batch files created during the first pass. These tuples were spilled to disk when they belonged to later batches.

The function includes important optimizations:
- **NULL Handling**: Tuples that produce NULL hash values are immediately discarded since they cannot match in hash joins
- **Empty Relation Detection**: Sets hj_OuterNotEmpty flag when valid tuples are found, used for optimization in rescans
- **Pre-fetched Tuple Management**: Handles the case where a tuple was fetched during empty-outer optimization

The function returns NULL when no more tuples are available in the current batch, signaling the hash join algorithm to either process unmatched inner tuples (for outer joins) or advance to the next batch.

## Parameters / Member Variables
- `*outerNode`: The outer plan node to fetch tuples from (during first pass)
- `*hjstate`: The HashJoinState containing join execution state
- `*hashvalue`: Output parameter to store the computed hash value of the returned tuple
## Dependencies
- Functions called/Symbols referenced:
  - TupIsNull: Checks if a tuple slot is empty
  - [ExecProcNode](ExecProcNode.md): Executes the outer plan node to get next tuple
  - [ExecHashGetHashValue](ExecHashGetHashValue.md): Computes hash value for the tuple
  - [ExecHashJoinGetSavedTuple](ExecHashJoinGetSavedTuple.md): Retrieves tuple from batch file
  - HJ_FILL_OUTER: Macro to check if this is an outer join

- Called from:
  - [ExecHashJoinImpl](ExecHashJoinImpl.md): Main hash join execution function (non-parallel path)

## Notes and Other Information
Key behavioral aspects:

- **Batch Processing**: The function's behavior differs significantly between batch 0 (direct tuple fetch) and later batches (file-based tuple retrieval)
- **Hash Value Management**: For batch 0, hash values are computed; for later batches, they are read from saved tuple metadata
- **Memory Efficiency**: By processing tuples in batches, the algorithm can handle joins larger than available memory
- **NULL Tuple Filtering**: Tuples producing NULL hash values are filtered out immediately, as they cannot participate in hash-based matching

The function is critical for the hybrid hash join algorithm's ability to gracefully degrade to a disk-based algorithm when memory is insufficient, while maintaining optimal performance for memory-resident cases.

Location: src/backend/executor/nodeHashjoin.c:890-963

## Simplified Source

```c
static TupleTableSlot *
ExecHashJoinOuterGetTuple(PlanState *outerNode,
                          HashJoinState *hjstate,
                          uint32 *hashvalue)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int curbatch = hashtable->curbatch;
    TupleTableSlot *slot;

    if (curbatch == 0) {  // First pass - get tuples from outer plan
        // Check for pre-fetched tuple from empty-outer optimization
        slot = hjstate->hj_FirstOuterTupleSlot;
        if (!TupIsNull(slot)) {
            hjstate->hj_FirstOuterTupleSlot = NULL;
        } else {
            slot = ExecProcNode(outerNode);
        }

        // Process tuples until we find one with a valid hash value
        while (!TupIsNull(slot)) {
            ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

            econtext->ecxt_outertuple = slot;
            if (ExecHashGetHashValue(hashtable, econtext,
                                   hjstate->hj_OuterHashKeys,
                                   true,  // outer tuple
                                   HJ_FILL_OUTER(hjstate),
                                   hashvalue)) {
                // Found valid tuple - remember outer relation is not empty
                hjstate->hj_OuterNotEmpty = true;
                return slot;
            }

            // Tuple had NULL hash value, skip it
            slot = ExecProcNode(outerNode);
        }
    } else if (curbatch < hashtable->nbatch) {  // Later batch - read from file
        BufFile *file = hashtable->outerBatchFile[curbatch];

        // Handle empty batch files (can happen in outer joins)
        if (file == NULL)
            return NULL;

        // Read saved tuple from batch file
        slot = ExecHashJoinGetSavedTuple(hjstate, file, hashvalue,
                                       hjstate->hj_OuterTupleSlot);
        if (!TupIsNull(slot))
            return slot;
    }

    // End of current batch
    return NULL;
}
```