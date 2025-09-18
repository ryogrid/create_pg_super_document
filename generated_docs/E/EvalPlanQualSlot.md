# EvalPlanQualSlot

## Location
src/backend/executor/execMain.c: 2600 - 2627

## Overview
EvalPlanQualSlot returns or creates a TupleTableSlot for EPQ (Eval Plan Qual) test tuples associated with a specific relation and range table index.

## Definition


## Detailed Description
This function provides access to tuple slots used during EPQ testing, which is part of PostgreSQL's mechanism for handling concurrent tuple modifications. It manages a lazy initialization pattern where slots are created only when first needed. The function looks up the appropriate slot in the epqstate's relsubs_slot array using the range table index (rti). If the slot doesn't exist, it creates a new one using the relation's tuple descriptor and adds it to the EPQ state's tuple table. Memory allocation is performed in the parent estate's query context to ensure proper lifetime management.

## Parameters / Member Variables
- : Pointer to the EPQState structure containing the slot array and parent estate information
- : The relation for which the tuple slot is needed
- : Range table index (1-based) identifying the specific relation within the query's range table

## Dependencies
- Functions called/Symbols referenced:
  - table_slot_create
  - EPQState
- Called from (representative examples):
  - EvalPlanQual
  - ExecLockRows
  - ExecDelete
  - ExecUpdate
  - ExecMergeMatched
  - ExecGetJunkAttribute

## Notes and Other Information
- Only requires EvalPlanQualInit() to have been called; EvalPlanQualBegin() is not necessary
- Uses lazy initialization - slots are created only when first accessed
- Memory context switching ensures proper memory management in the query context
- The rti parameter is 1-based but converted to 0-based for array indexing
- Includes assertions to validate relation and range table index bounds
- Part of PostgreSQL's MVCC (Multi-Version Concurrency Control) infrastructure