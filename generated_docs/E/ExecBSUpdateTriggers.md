# ExecBSUpdateTriggers

## Location
src/backend/commands/trigger.c: 2906 - 2963

## Overview
ExecBSUpdateTriggers executes BEFORE STATEMENT UPDATE triggers, which fire once per UPDATE statement before any rows are modified.

## Definition


## Detailed Description
This function executes BEFORE STATEMENT UPDATE triggers, which are fired once per UPDATE statement before any individual rows are processed. These triggers operate at the statement level rather than the row level, making them suitable for operations that need to occur once per statement regardless of how many rows will be affected.

The function first checks if there are any BEFORE STATEMENT UPDATE triggers defined and whether they haven't already been fired in the current context (to avoid duplicate execution). It retrieves information about which columns are being updated using ExecGetAllUpdatedCols and passes this information to each trigger.

BEFORE STATEMENT triggers are not allowed to return values - if a trigger attempts to do so, an error is raised. These triggers are typically used for logging, security checks, or other operations that should occur once per statement.

## Parameters / Member Variables
- : Executor state containing execution context and memory management information
- : ResultRelInfo containing relation metadata and trigger information (must be the root relation, not a partition)

## Dependencies
- Functions called/Symbols referenced:
  - before_stmt_triggers_fired
  - [ExecGetAllUpdatedCols](ExecGetAllUpdatedCols.md)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - GetPerTupleMemoryContext
- Data types referenced:
  - TriggerDesc
  - TriggerData
  - Trigger
  - [Bitmapset](../B/Bitmapset.md)
  - TRIGGER_EVENT_UPDATE
  - TRIGGER_EVENT_BEFORE
  - TRIGGER_TYPE_STATEMENT
  - TRIGGER_TYPE_BEFORE
  - TRIGGER_TYPE_UPDATE
  - CMD_UPDATE
- Macros used:
  - TRIGGER_TYPE_MATCHES
- Called from (representative examples):
  - [fireBSTriggers](../f/fireBSTriggers.md) (in nodeModifyTable.c)

## Notes and Other Information
- Only executes on the root relation (parent table), not on partitions
- Includes duplicate execution prevention through before_stmt_triggers_fired check
- Raises an error if any trigger attempts to return a non-NULL value
- Passes updated column information to triggers via tg_updatedcols
- Returns void as statement-level triggers cannot modify the operation
- Part of the statement-level trigger execution system
- Located in src/backend/commands/trigger.c:2906-2963
- Operates before any row-level processing begins for UPDATE statements