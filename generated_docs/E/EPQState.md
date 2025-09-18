# EPQState

## Location
src/include/nodes/execnodes.h: 1252 - 1313

## Overview
EPQState manages the execution state for EvalPlanQual (EPQ) operations, which recheck candidate tuples during concurrent modifications to ensure transaction isolation and consistency.

## Definition


## Detailed Description
EPQState implements EvalPlanQual (EPQ) rechecking, a critical mechanism in PostgreSQL's Multi-Version Concurrency Control (MVCC) system. When a transaction attempts to update or delete a tuple that has been concurrently modified by another transaction, EPQ creates a separate execution environment to recheck the candidate tuple against the original query conditions. This ensures that the query's WHERE clause and join conditions are still satisfied after concurrent modifications, maintaining transaction isolation without unnecessary blocking.

## Parameters / Member Variables
- : Pointer to the main query's execution state, providing shared resources like range tables
- : Parameter ID used to force re-evaluation of scan nodes during EPQ execution
- : List of range table indexes for relations that may need EPQ rechecking
- : Tuple table structure managing the relsubs_slot array
- : Array of slots containing EPQ test tuples, indexed by scanrelid - 1
- : Plan tree that needs to be rechecked during EPQ execution
- : List of ExecAuxRowMarks for non-locking row marking operations
- : Original output tuple being rechecked, set before EPQ evaluation begins
- : Separate EState for EPQ execution, sharing resources with parentestate
- : Array of row marks that can be fetched on-demand, indexed by scanrelid - 1
- : Array of flags indicating whether EPQ tuple has been fetched for each relation
- : Array of flags indicating relations with no EPQ tuple during current test
- : Execution state tree for the plan being rechecked, separate from main query

## Dependencies
- Functions called/Symbols referenced:
  - EState
  - List
  - TupleTableSlot
  - Plan
  - ExecAuxRowMark
  - PlanState
- Called from (representative examples):
  - EvalPlanQual
  - EvalPlanQualInit
  - EvalPlanQualBegin
  - EvalPlanQualNext
  - ExecMergeMatched
  - GetTupleForTrigger

## Notes and Other Information
EPQState is fundamental to PostgreSQL's optimistic concurrency control, allowing transactions to proceed without locking while maintaining consistency through rechecking. The separate execution environment (recheckestate) enables EPQ to run modified plans that use substitute tuples instead of scanning base tables. The relsubs_slot mechanism allows callers to provide specific tuples for rechecking, while the rowmark system handles tuple identification. EPQ is essential for operations like UPDATE, DELETE, MERGE, and trigger execution where concurrent modifications must be handled gracefully. The blocked/done flag arrays optimize EPQ execution by tracking which relations have viable tuples for the current recheck operation.