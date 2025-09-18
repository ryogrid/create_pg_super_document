# EvalPlanQualNext

## Location
[src/backend/executor/execMain.c:2739-2754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L2739-L2754)

## Overview
EvalPlanQualNext fetches the next row from an ongoing EPQ (Eval Plan Qual) testing operation by executing the recheck plan state.

## Definition


## Detailed Description
This function advances EPQ testing by executing one iteration of the recheck plan state and returning the resulting tuple slot. It operates within the appropriate memory context (the recheck estate's query context) to ensure proper memory management during plan execution. The function is designed to be called repeatedly to iterate through potential matching rows during EPQ processing, though in practice there should rarely be more than one row to process. It serves as the core execution driver for EPQ operations after the EPQ state has been properly initialized and begun.

## Parameters / Member Variables
- : Pointer to the EPQState containing the recheck plan state and estate information

## Dependencies
- Functions called/Symbols referenced:
  - ExecProcNode
  - [EPQState](EPQState.md)
- Called from (representative examples):
  - [EvalPlanQual](EvalPlanQual.md)
  - [ExecLockRows](ExecLockRows.md)
  - EvalPlanQualSetSlot

## Notes and Other Information
- The comment indicates that in practice there should never be more than one row returned
- Performs memory context switching to ensure execution happens in the correct context
- Returns the result of executing the recheck plan state via ExecProcNode
- Part of the iterative EPQ processing workflow
- Must be called after EvalPlanQualBegin has initialized the recheck plan state
- Returns NULL when no more tuples are available from the EPQ recheck