# EvalPlanQualInit

## Location
src/backend/executor/execMain.c: 2541 - 2582

## Overview
Initializes the EPQState structure during plan state node creation to prepare for potential EvalPlanQual processing in concurrent update scenarios.

## Definition
void EvalPlanQualInit(EPQState *epqstate, EState *parentestate, Plan *subplan, List *auxrowmarks, int epqParam, List *resultRelations)

## Detailed Description
EvalPlanQualInit sets up the foundational state required for EvalPlanQual operations, which are used to handle concurrent tuple modifications under READ COMMITTED isolation. The function initializes an EPQState structure with the necessary context and allocates resources that will be needed for potential EPQ rechecking operations.

Key initialization activities include:
- Storing references to the parent execution state and plan structures
- Pre-allocating an array of tuple table slots for each potential range table entry
- Setting up the parameter and relation context for EPQ operations
- Marking the EPQ state as inactive until actually needed

The function is designed to minimize overhead by pre-allocating only essential resources, deferring more expensive initialization until EvalPlanQualBegin() is called when EPQ processing is actually required.

## Parameters / Member Variables
- `epqstate`: EPQState structure to be initialized for future EvalPlanQual operations
- `parentestate`: Parent execution state containing range table and execution context
- `subplan`: Plan tree that will be used for EPQ rechecking (can be NULL if set later)
- `auxrowmarks`: List of auxiliary row marks for row locking (can be NIL if set later)
- `epqParam`: Parameter ID used for EPQ parameter passing
- `resultRelations`: List of range table indexes that are potential EPQ target relations

## Dependencies
- Functions called/Symbols referenced:
  - EPQState (structure type)
  - EState (structure type)
  - Plan (structure type)
  - TupleTableSlot (structure type)
  - palloc0
  - NIL
- Called from (representative examples):
  - ExecInitLockRows
  - ExecInitModifyTable
  - apply_handle_update_internal
  - apply_handle_delete_internal
  - apply_handle_tuple_routing

## Notes and Other Information
This function is part of the initialization phase of PostgreSQL's EvalPlanQual system, which provides snapshot-based concurrency control under READ COMMITTED isolation. The early allocation of relsubs_slot array allows EvalPlanQualSlot() to be used for holding tuples that may require EPQ processing without forcing the full overhead of EvalPlanQualBegin(). The function is commonly called during the initialization of plan nodes that may need to handle concurrent modifications, such as ModifyTable and LockRows nodes. The resultRelations parameter helps optimize EPQ processing by identifying which relations are actual targets versus those that should return empty results during EPQ rechecking.