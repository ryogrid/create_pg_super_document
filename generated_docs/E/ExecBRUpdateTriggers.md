# ExecBRUpdateTriggers

## Location
[src/backend/commands/trigger.c:3147-3170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3147-L3170)

## Overview
An ABI-compatible wrapper function that provides backward compatibility for the old interface to BEFORE ROW UPDATE trigger execution by delegating to ExecBRUpdateTriggersNew.

## Definition

```c
bool
ExecBRUpdateTriggers(EState *estate, EPQState *epqstate,
					 ResultRelInfo *relinfo,
					 ItemPointer tupleid,
					 HeapTuple fdw_trigtuple,
					 TupleTableSlot *newslot,
					 TM_Result *tmresult,
					 TM_FailureData *tmfd)
```
## Detailed Description
This function serves as a backward-compatibility wrapper for the older interface to BEFORE ROW UPDATE trigger execution. It simply forwards all parameters to ExecBRUpdateTriggersNew with is_merge_update set to false, maintaining ABI compatibility for existing code while ensuring that new functionality is centralized in the newer function.

## Parameters / Member Variables
- `*estate`: Executor state containing execution context and memory management
- `*epqstate`: EPQ state for handling concurrent tuple modifications
- `*relinfo`: Relation information including trigger descriptors and metadata
- `tupleid`: ItemPointer to the target tuple on disk (NULL if using fdw_trigtuple)
- `fdw_trigtuple`: Pre-supplied tuple from FDW (NULL if using tupleid)
- `*newslot`: TupleTableSlot containing the new tuple values after update
- `*tmresult`: Output parameter for tuple manager operation result
- `*tmfd`: Output parameter for tuple manager failure data
## Dependencies
- Functions called/Symbols referenced:
  - [ExecBRUpdateTriggersNew](ExecBRUpdateTriggersNew.md)
- Called from (representative examples):
  - [ExecSimpleRelationUpdate](ExecSimpleRelationUpdate.md)

## Notes and Other Information
- Marked as deprecated for new code - use ExecBRUpdateTriggersNew directly instead
- Always passes is_merge_update as false, indicating standard UPDATE behavior
- Maintains backward compatibility for existing callers that don't need MERGE-specific behavior
- Return value and semantics are identical to ExecBRUpdateTriggersNew

## Simplified Source

```c
bool ExecBRUpdateTriggers(EState *estate, EPQState *epqstate,
                         ResultRelInfo *relinfo, ItemPointer tupleid,
                         HeapTuple fdw_trigtuple, TupleTableSlot *newslot,
                         TM_Result *tmresult, TM_FailureData *tmfd) {
    // Simple wrapper for backward compatibility - delegates to new version
    return ExecBRUpdateTriggersNew(estate, epqstate, relinfo, tupleid,
                                  fdw_trigtuple, newslot, tmresult, tmfd,
                                  false /* is_merge_update */);
}
```