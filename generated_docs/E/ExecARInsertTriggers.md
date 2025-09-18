# ExecARInsertTriggers

## Location
[src/backend/commands/trigger.c:2536-2561](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2536-L2561)

## Overview
Executes AFTER ROW INSERT triggers for a given relation and handles transition table capture for inserted tuples.

## Definition


## Detailed Description
ExecARInsertTriggers is responsible for executing AFTER ROW INSERT triggers and managing transition table capture for insert operations. The function validates that foreign tables don't use transition table capture (which is not supported) and then delegates the actual trigger execution to AfterTriggerSaveEvent. This function is part of PostgreSQL's deferred trigger execution system, where AFTER triggers are queued for execution at the end of the statement or transaction.

The function performs a critical validation check for foreign tables with transition capture, raising an error if this unsupported combination is detected. When appropriate triggers exist or transition capture is required, it saves the trigger event for later execution during the commit phase.

## Parameters / Member Variables
- : Execution state containing transaction and query context information
- : Information about the target relation including trigger descriptors and FDW routines
- : TupleTableSlot containing the newly inserted tuple data
- : List of indexes that need to be rechecked after the insert operation
- : State information for capturing tuples into transition tables for trigger OLD/NEW TABLE references

## Dependencies
- Functions called/Symbols referenced:
  - AfterTriggerSaveEvent
  - TRIGGER_EVENT_INSERT
- Data structures used:
  - TransitionCaptureState
  - TriggerDesc
- Called from (representative examples):
  - [CopyMultiInsertBufferFlush](../C/CopyMultiInsertBufferFlush.md)
  - [CopyFrom](../C/CopyFrom.md)
  - [ExecSimpleRelationInsert](ExecSimpleRelationInsert.md)
  - [ExecInsert](ExecInsert.md)
  - [ExecBatchInsert](ExecBatchInsert.md)

## Notes and Other Information
- This function is part of PostgreSQL's deferred trigger execution system
- Foreign tables do not support transition table capture, and the function explicitly checks and errors for this condition
- The function only saves trigger events when there are actual AFTER ROW INSERT triggers defined or when transition capture is needed
- AFTER triggers are executed later during statement/transaction completion rather than immediately
- The recheckIndexes parameter is passed through to support index constraint validation that may be required after trigger execution