# ExecHashTableDestroy

## Location
src/backend/executor/nodeHash.c: 883 - 915

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
  - BufFileClose (closes temporary batch files)
  - MemoryContextDelete (deallocates memory contexts)
  - pfree (frees the control structure)
- Called from (representative examples):
  - ExecEndHashJoin (hash join cleanup during query termination)
  - ExecReScanHashJoin (hash join reset for parameter changes)

## Notes and Other Information
- Temporary files are only closed for batches 1 and higher since batch 0 never uses temporary storage
- Parallel hash joins do not use the innerBatchFile/outerBatchFile arrays, so file cleanup is skipped for them
- Memory context deletion automatically handles all child contexts (batchCxt, spillCxt) and their allocations
- The function is safe to call even if some temporary files were never opened or if the hash table was only partially initialized
- Part of the hash join cleanup sequence that ensures no resources are leaked when queries complete or are aborted