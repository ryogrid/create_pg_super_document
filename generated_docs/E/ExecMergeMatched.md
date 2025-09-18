# ExecMergeMatched

## Location
src/backend/executor/nodeModifyTable.c: 2890 - 3400

## Overview
Handles execution of WHEN MATCHED and WHEN NOT MATCHED BY SOURCE actions in MERGE statements, including concurrent update detection and recovery logic.

## Definition


## Detailed Description
ExecMergeMatched is responsible for executing the first qualifying WHEN MATCHED or WHEN NOT MATCHED BY SOURCE action in a MERGE statement. It handles complex scenarios involving concurrent modifications during MERGE execution:

1. **Action Selection**: Tests join conditions to determine whether to process WHEN MATCHED or WHEN NOT MATCHED BY SOURCE actions
2. **Concurrent Update Handling**: Detects and adapts to concurrent updates that may change match status during execution
3. **Action Execution**: Performs UPDATE, DELETE, or DO NOTHING operations based on the qualifying action
4. **Recovery Logic**: Uses EvalPlanQual (EPQ) to handle concurrent modifications and re-evaluate conditions

The function can restart processing from the beginning when concurrent updates are detected, potentially switching from MATCHED to NOT MATCHED BY SOURCE actions. It ensures forward progress by following update chains and never switches back to MATCHED actions once processing NOT MATCHED BY SOURCE actions.

## Parameters / Member Variables
- : ModifyTableContext containing execution state and context information
- : ResultRelInfo structure with information about the target relation
- : ItemPointer to the target tuple for table-based operations (NULL for view operations)
- : HeapTuple representing the target tuple for view-based operations (NULL for table operations)
- : Boolean indicating whether command tags can be set during execution
- : Pointer to boolean that tracks match status; may be modified to false if concurrent updates cause tuples to no longer match

## Dependencies
- Functions called/Symbols referenced:
  - ExecQual
  - ExecProject
  - [ExecUpdatePrologue](ExecUpdatePrologue.md)
  - [ExecUpdateAct](ExecUpdateAct.md)
  - [ExecUpdateEpilogue](ExecUpdateEpilogue.md)
  - [ExecDeletePrologue](ExecDeletePrologue.md)
  - [ExecDeleteAct](ExecDeleteAct.md)
  - [ExecDeleteEpilogue](ExecDeleteEpilogue.md)
  - [ExecProcessReturning](ExecProcessReturning.md)
  - [EvalPlanQual](EvalPlanQual.md)
  - [EvalPlanQualSlot](EvalPlanQualSlot.md)
  - table_tuple_lock
  - table_tuple_fetch_row_version
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
- Called from (representative examples):
  - [ExecMerge](ExecMerge.md)

## Notes and Other Information
- Implements sophisticated concurrent update handling using EvalPlanQual mechanism
- Supports Row Level Security (RLS) policy checks for UPDATE and DELETE operations
- Handles INSTEAD OF triggers for view operations
- Can process both regular table operations and view operations based on input parameters
- Uses tuple locking mechanisms to ensure consistency during concurrent access
- Maintains statistics counters for merged updated and deleted tuples
- Implements proper error handling for serialization failures and cardinality violations
- The function may loop back to reprocess actions when concurrent updates are detected (goto lmerge_matched)
- Supports cross-partition updates by detecting and handling partition movement scenarios