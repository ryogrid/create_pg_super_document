# FreeExecutorState

## Location
src/backend/executor/execUtils.c: 189 - 233

## Overview
Releases an EState along with all remaining working storage, performing proper cleanup of expression contexts, JIT resources, and the per-query memory context.

## Definition


## Detailed Description
FreeExecutorState is responsible for the complete cleanup and deallocation of an EState structure and all its associated resources. The function performs a multi-step cleanup process that includes shutting down active ExprContexts, releasing JIT compilation contexts, destroying partition directories, and finally deleting the entire per-query memory context.

The function is designed to handle cleanup in situations where the EState has been used for expression evaluation and not necessarily for running a complete Plan. It ensures that any remaining shutdown callbacks get called, which is critical for releasing resources that aren't simply memory within the per-query context. The function can be called from any memory context as long as it's not one of the contexts being freed.

## Parameters / Member Variables
- : Pointer to the EState structure to be freed and cleaned up

Key cleanup operations performed:
- Iteratively frees all ExprContexts in  list
- Releases JIT context if  is allocated
- Destroys partition directory if  exists  
- Deletes the entire per-query memory context 

## Dependencies
- Functions called/Symbols referenced:
  - FreeExprContext
  - jit_release_context
  - DestroyPartitionDirectory
  - MemoryContextDelete
  - linitial (list manipulation macro)

- Called from (representative examples):
  - standard_ExecutorEnd
  - EvalPlanQualEnd
  - evaluate_expr
  - CopyFrom
  - compute_index_stats
  - ATRewriteTable
  - IndexCheckExclusion

## Notes and Other Information
The function uses a while loop to iteratively free ExprContexts, with a comment noting that using repeated list_delete() operations might not be the most efficient approach. The cleanup is performed in a specific order: first ExprContexts (which may have shutdown callbacks), then JIT resources, then partition directories, and finally the memory context itself. This ordering ensures that any cleanup callbacks can still access memory that might be needed during the shutdown process. The function is not responsible for releasing non-memory resources like open relations or buffer pins - that cleanup must be handled elsewhere in the executor shutdown sequence.