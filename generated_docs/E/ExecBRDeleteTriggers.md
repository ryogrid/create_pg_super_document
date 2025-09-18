# ExecBRDeleteTriggers

## Location
src/backend/commands/trigger.c: 2794 - 2811

## Overview
ExecBRDeleteTriggers is an ABI-compatible wrapper function that executes BEFORE ROW DELETE triggers during DELETE operations, providing backward compatibility with older PostgreSQL versions.

## Definition


## Detailed Description
This function serves as a compatibility wrapper around the newer ExecBRDeleteTriggersNew function. It maintains backward compatibility by calling the new implementation with the  parameter set to false. The function handles the execution of BEFORE ROW DELETE triggers, which are fired before a row is deleted from a table. These triggers can potentially suppress the delete operation by returning NULL.

The function delegates all actual trigger processing to ExecBRDeleteTriggersNew, ensuring that existing code using the old function signature continues to work without modification.

## Parameters / Member Variables
- : Executor state containing execution context and memory management information
- : EvalPlanQual state for handling concurrent tuple updates during trigger execution
- : ResultRelInfo containing relation metadata and trigger information
- : ItemPointer identifying the tuple to be deleted (used for regular tables)
- : HeapTuple for foreign data wrapper tables (alternative to tupleid)
- : Output parameter for returning concurrently updated tuple slot
- : Output parameter for table access method result status
- : Output parameter for failure data when tuple access fails

## Dependencies
- Functions called/Symbols referenced:
  - ExecBRDeleteTriggersNew
- Data types referenced:
  - EPQState
  - TM_Result
  - TM_FailureData
- Called from (representative examples):
  - ExecSimpleRelationDelete

## Notes and Other Information
- This is explicitly marked as an ABI-compatible wrapper and should not be used in new code
- The function always calls ExecBRDeleteTriggersNew with is_merge_delete=false
- Returns true if the delete operation should proceed, false if triggers suppressed the delete
- Located in src/backend/commands/trigger.c:2794-2811
- Part of PostgreSQL's trigger execution subsystem for DELETE operations