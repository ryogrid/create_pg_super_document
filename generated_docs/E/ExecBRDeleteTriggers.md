# ExecBRDeleteTriggers

## Location
[src/backend/commands/trigger.c:2794-2811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2794-L2811)

## Overview
ExecBRDeleteTriggers is an ABI-compatible wrapper function that executes BEFORE ROW DELETE triggers during DELETE operations, providing backward compatibility with older PostgreSQL versions.

## Definition

```c
bool
ExecBRDeleteTriggers(EState *estate, EPQState *epqstate,
					 ResultRelInfo *relinfo,
					 ItemPointer tupleid,
					 HeapTuple fdw_trigtuple,
					 TupleTableSlot **epqslot,
					 TM_Result *tmresult,
					 TM_FailureData *tmfd)
```
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
  - [ExecBRDeleteTriggersNew](ExecBRDeleteTriggersNew.md)
- Data types referenced:
  - [EPQState](EPQState.md)
  - TM_Result
  - [TM_FailureData](../T/TM_FailureData.md)
- Called from (representative examples):
  - [ExecSimpleRelationDelete](ExecSimpleRelationDelete.md)

## Notes and Other Information
- This is explicitly marked as an ABI-compatible wrapper and should not be used in new code
- The function always calls ExecBRDeleteTriggersNew with is_merge_delete=false
- Returns true if the delete operation should proceed, false if triggers suppressed the delete
- Located in src/backend/commands/trigger.c:2794-2811
- Part of PostgreSQL's trigger execution subsystem for DELETE operations

## Simplified Source

```c
bool ExecBRDeleteTriggers(EState *estate, EPQState *epqstate,
                         ResultRelInfo *relinfo, ItemPointer tupleid,
                         HeapTuple fdw_trigtuple, TupleTableSlot **epqslot,
                         TM_Result *tmresult, TM_FailureData *tmfd) {
    // Simple wrapper for backward compatibility - delegates to new version
    return ExecBRDeleteTriggersNew(estate, epqstate, relinfo, tupleid,
                                  fdw_trigtuple, epqslot, tmresult, tmfd,
                                  false /* is_merge_delete */);
}
```