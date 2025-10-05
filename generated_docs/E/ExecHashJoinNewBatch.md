# ExecHashJoinNewBatch

## Location
[src/backend/executor/nodeHashjoin.c:1031-1171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1031-L1171)

## Overview
Switches to a new hashjoin batch when the current batch has been completed, managing the transition between batches in a multi-batch hash join operation.

## Definition

```c
static bool
ExecHashJoinNewBatch(HashJoinState *hjstate)
```
## Detailed Description
This function manages the complex process of transitioning from one batch to the next in a hash join operation. Hash joins may be divided into multiple batches when the inner relation is too large to fit entirely in memory. The function handles cleanup of the previous batch, determines which batches can be skipped based on outer join requirements and optimization rules, reloads the hash table with the new inner batch data, and prepares the outer batch file for reading.

The function implements several optimization strategies:
- Skips completely empty batches on both sides when possible
- Handles special cases for outer joins where empty batches still need processing
- Manages skew optimization state transitions after the first batch
- Accounts for dynamic batch number increases during execution

## Parameters / Member Variables
- `*hjstate`: The HashJoinState containing all state information for the hash join operation, including the hash table and batch file references
## Dependencies
- Functions called/Symbols referenced:
  - [BufFileClose](../B/BufFileClose.md)
  - HJ_FILL_OUTER
  - HJ_FILL_INNER
  - [ExecHashTableReset](ExecHashTableReset.md)
  - [BufFileSeek](../B/BufFileSeek.md)
  - [ExecHashJoinGetSavedTuple](ExecHashJoinGetSavedTuple.md)
  - [ExecHashTableInsert](ExecHashTableInsert.md)
- Called from (representative examples):
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)

## Notes and Other Information
- Returns true if a new batch was successfully prepared, false if no more batches remain
- Implements three key rules for determining when batches can be skipped: outer join requirements, dynamic batch increases during inner scan, and dynamic batch increases during outer scan
- Manages memory cleanup by closing previous batch files and resetting skew optimization state
- The function is static and only used internally within the hash join executor node
- Critical for memory management in large hash join operations that exceed available memory

## Simplified Source

```c
static bool
ExecHashJoinNewBatch(HashJoinState *hjstate)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int nbatch = hashtable->nbatch;
    int curbatch = hashtable->curbatch;
    BufFile *innerFile;
    TupleTableSlot *slot;
    uint32 hashvalue;

    // Clean up previous batch file
    if (curbatch > 0) {
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
    } else {
        // First batch completed - disable skew optimization
        hashtable->skewEnabled = false;
        hashtable->skewBucket = NULL;
        hashtable->skewBucketNums = NULL;
        hashtable->nSkewBuckets = 0;
        hashtable->spaceUsedSkew = 0;
    }

    // Find next processable batch, skipping empty ones when possible
    curbatch++;
    while (curbatch < nbatch &&
           (hashtable->outerBatchFile[curbatch] == NULL ||
            hashtable->innerBatchFile[curbatch] == NULL)) {

        // Check if we must process this batch despite being partially empty:
        // 1. Outer joins need empty inner batches processed
        // 2. Dynamic batch increases require processing for tuple reassignment

        if (hashtable->outerBatchFile[curbatch] && HJ_FILL_OUTER(hjstate))
            break;  // Must process for outer join
        if (hashtable->innerBatchFile[curbatch] && HJ_FILL_INNER(hjstate))
            break;  // Must process for inner join
        if (hashtable->innerBatchFile[curbatch] &&
            nbatch != hashtable->nbatch_original)
            break;  // Must process due to dynamic batch increase
        if (hashtable->outerBatchFile[curbatch] &&
            nbatch != hashtable->nbatch_outstart)
            break;  // Must process due to outer scan batch increase

        // Skip this batch - clean up files
        if (hashtable->innerBatchFile[curbatch])
            BufFileClose(hashtable->innerBatchFile[curbatch]);
        hashtable->innerBatchFile[curbatch] = NULL;
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
        curbatch++;
    }

    if (curbatch >= nbatch)
        return false;  // No more batches

    hashtable->curbatch = curbatch;

    // Reset hash table and load new inner batch
    ExecHashTableReset(hashtable);
    innerFile = hashtable->innerBatchFile[curbatch];

    if (innerFile != NULL) {
        // Rewind inner batch file and reload hash table
        BufFileSeek(innerFile, 0, 0, SEEK_SET);

        while ((slot = ExecHashJoinGetSavedTuple(hjstate, innerFile,
                                               &hashvalue,
                                               hjstate->hj_HashTupleSlot))) {
            // Insert tuple into hash table (may trigger further batching)
            ExecHashTableInsert(hashtable, slot, hashvalue);
        }

        // Clean up inner batch file
        BufFileClose(innerFile);
        hashtable->innerBatchFile[curbatch] = NULL;
    }

    // Prepare outer batch file for reading
    if (hashtable->outerBatchFile[curbatch] != NULL) {
        BufFileSeek(hashtable->outerBatchFile[curbatch], 0, 0, SEEK_SET);
    }

    return true;
}
```