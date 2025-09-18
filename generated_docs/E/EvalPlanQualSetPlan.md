# EvalPlanQualSetPlan

## Location
[src/backend/executor/execMain.c:2583-2599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L2583-L2599)

## Overview
EvalPlanQualSetPlan sets or changes the subplan of an EPQState structure, which is used in PostgreSQL's Eval Plan Qual (EPQ) mechanism for handling concurrent updates during query execution.

## Definition


## Detailed Description
This function is part of PostgreSQL's EPQ (Eval Plan Qual) infrastructure, which handles concurrent tuple modifications during query execution. EvalPlanQualSetPlan allows modification of an existing EPQState by setting or changing its associated subplan and auxiliary row marks. The function first ensures that any currently running EPQ query is properly shut down before updating the plan configuration. Originally designed to handle multiple subplans in ModifyTable operations, this function could potentially be refactored as the codebase has evolved.

## Parameters / Member Variables
- : Pointer to the EPQState structure that manages EPQ execution state
- : The new Plan node to be associated with the EPQ state
- : List of auxiliary row marks that depend on the specified plan

## Dependencies
- Functions called/Symbols referenced:
  - [EvalPlanQualEnd](EvalPlanQualEnd.md)
  - [EPQState](EPQState.md)
- Called from (representative examples):
  - [ExecInitModifyTable](ExecInitModifyTable.md)
  - ExecGetJunkAttribute

## Notes and Other Information
- The function includes a comment noting that it was originally needed for ModifyTable to handle multiple subplans
- The implementation suggests this could be refactored out of existence in modern PostgreSQL versions
- The function ensures proper cleanup by calling EvalPlanQualEnd before making changes
- Row marks are plan-dependent and must be updated together with the plan change