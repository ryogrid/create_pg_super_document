# ExecMergeNotMatched

## Location
src/backend/executor/nodeModifyTable.c: 3401 - 3483

## Overview
Executes the first qualifying WHEN NOT MATCHED [BY TARGET] action in MERGE statements for source tuples that have no corresponding target tuple.

## Definition


## Detailed Description
ExecMergeNotMatched handles the NOT MATCHED BY TARGET case in MERGE statements, which occurs when a source tuple has no corresponding target tuple based on the join conditions. This function:

1. **Action Processing**: Iterates through the list of WHEN NOT MATCHED [BY TARGET] actions and executes the first one whose WHEN condition is satisfied
2. **INSERT Operations**: Primarily handles INSERT operations since NOT MATCHED cases typically involve inserting new rows from source data
3. **Tuple Projection**: Projects the new tuple using the action's projection, which combines source data according to the INSERT's target list
4. **Partitioned Table Support**: Handles partitioned tables by using the root relation's descriptor for tuple projection

The function is simpler than ExecMergeMatched because NOT MATCHED cases don't need to handle concurrent updates - there's no existing target tuple to be modified concurrently.

## Parameters / Member Variables
- : ModifyTableContext containing execution state and context information
- : ResultRelInfo structure with information about the target relation
- : Boolean indicating whether command tags can be set during execution

## Dependencies
- Functions called/Symbols referenced:
  - ExecQual
  - ExecProject
  - ExecInsert
  - MergeActionState
  - MERGE_WHEN_NOT_MATCHED_BY_TARGET
- Called from (representative examples):
  - ExecMerge
  - ExecModifyTable

## Notes and Other Information
- Only processes INSERT and DO NOTHING actions since these are the only valid operations for NOT MATCHED cases
- Uses only the source tuple (context->planSlot) for condition evaluation and projection since target tuples don't exist
- Maintains the mt_merge_inserted counter for inserted tuples
- Stops processing after the first qualifying action is found and executed (required SQL standard behavior)
- Simpler than matched case handling due to absence of concurrency concerns with target tuples
- Works efficiently with partitioned tables by using root relation descriptors
- The comment suggests potential optimization for partitioned tables by avoiding copies of actionStates for not-matched actions