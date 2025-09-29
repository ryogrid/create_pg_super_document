# ExecHashTableDestroy

## Location
[src/backend/executor/nodeHash.c:883-915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L883-L915)

## Overview
Destroys a hash table by closing temporary files, deallocating memory contexts, and freeing the control structure.

## Definition
void ExecHashTableDestroy(HashJoinTable hashtable)

## Detailed Description
ExecHashTableDestroy performs comprehensive cleanup of a hash table data structure used in hash join operations. The function ensures proper resource deallocation by systematically closing any open temporary batch files (used for spilling data when memory is insufficient), deleting the memory contexts that contain all hash table data, and finally freeing the main control structure.

The function handles multi-batch scenarios where temporary files may have been created for batch processing. It skips batch 0 since that batch is always processed in memory and never requires temporary files. The cleanup is designed to be safe for both regular and parallel hash joins, with parallel hash joins using shared tuplestores instead of individual temporary files.

Memory cleanup is performed hierarchically through memory context deletion. Since batchCxt and spillCxt are child contexts of hashCxt, deleting hashCxt automatically cleans up all associated memory allocations including bucket arrays, hash function information, and temporary data structures.

## Parameters / Member Variables
- hashtable: HashJoinTable structure to be destroyed, containing all hash table state and resources

## Dependencies
- Functions called/Symbols referenced:
  - [BufFileClose](../B/BufFileClose.md) (closes temporary batch files)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (deallocates memory contexts)
  - [pfree](../p/pfree.md) (frees the control structure)
- Called from (representative examples):
  - [ExecEndHashJoin](ExecEndHashJoin.md) (hash join cleanup during query termination)
  - [ExecReScanHashJoin](ExecReScanHashJoin.md) (hash join reset for parameter changes)

## Notes and Other Information
- Temporary files are only closed for batches 1 and higher since batch 0 never uses temporary storage
- Parallel hash joins do not use the innerBatchFile/outerBatchFile arrays, so file cleanup is skipped for them
- Memory context deletion automatically handles all child contexts (batchCxt, spillCxt) and their allocations
- The function is safe to call even if some temporary files were never opened or if the hash table was only partially initialized
- Part of the hash join cleanup sequence that ensures no resources are leaked when queries complete or are aborted

## Simplified Source

```c
void ExecHashTableDestroy(HashJoinTable hashtable) {
    // Close temporary batch files (skip batch 0, it has no temp files)
    if (hashtable->innerBatchFile != NULL) {
        for (int i = 1; i < hashtable->nbatch; i++) {
            if (hashtable->innerBatchFile[i])
                BufFileClose(hashtable->innerBatchFile[i]);
            if (hashtable->outerBatchFile[i])
                BufFileClose(hashtable->outerBatchFile[i]);
        }
    }

    // Release all working memory (including child contexts)
    MemoryContextDelete(hashtable->hashCxt);

    // Free the control structure
    pfree(hashtable);
}
```