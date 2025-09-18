# EvalPlanQualBegin

## Location
src/backend/executor/execMain.c: 2755 - 2821

## Overview
EvalPlanQualBegin initializes or resets an EPQ (Eval Plan Qual) state tree, preparing it for execution by either creating a new child EState or resetting an existing one.

## Definition


## Detailed Description
This function prepares an EPQ state for execution by handling two scenarios: initial setup and reset operations. On first invocation, it creates a new child EState by calling EvalPlanQualStart. For subsequent calls, it resets the existing EPQ infrastructure by copying the relsubs_blocked flags to relsubs_done to prevent fetching from blocked relations, synchronizing parameter values from the parent estate, and marking the plan tree for rescan. The function ensures that InitPlan outputs are properly evaluated and that parameter values are synchronized between parent and child execution states. It also sets up the plan tree to be rescanned by adding the EPQ parameter to the change parameter bitmap.

## Parameters / Member Variables
- : Pointer to the EPQState structure to be initialized or reset

## Dependencies
- Functions called/Symbols referenced:
  - [EvalPlanQualStart](EvalPlanQualStart.md)
  - [ExecSetParamPlanMulti](ExecSetParamPlanMulti.md)
  - GetPerTupleExprContext
  - [bms_add_member](../b/bms_add_member.md)
  - [EPQState](EPQState.md)
- Called from (representative examples):
  - [EvalPlanQual](EvalPlanQual.md)
  - [ExecLockRows](ExecLockRows.md)
  - [ExecDelete](ExecDelete.md)
  - EvalPlanQualSetSlot

## Notes and Other Information
- Handles both initialization (first call) and reset (subsequent calls) scenarios
- Synchronizes parameter values between parent and child execution states
- Forces evaluation of InitPlan outputs that may be needed by the subplan
- Copies relsubs_blocked to relsubs_done to prevent fetching from blocked relations
- Marks the plan tree for rescan by setting the change parameter bitmap
- Must be called before EvalPlanQualNext to ensure proper EPQ execution state
- Part of PostgreSQL's MVCC infrastructure for handling concurrent updates
- Creates the execution environment needed for EPQ tuple validation