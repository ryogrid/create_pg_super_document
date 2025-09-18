# GetAfterTriggersTableData

## Location
src/backend/commands/trigger.c: 4883 - 4919

## Overview
Finds or creates an AfterTriggersTableData structure for a specified trigger event (relation + operation type combination), ensuring that closed structures are ignored to prevent additional tuple insertion.

## Definition
```c
static AfterTriggersTableData *GetAfterTriggersTableData(Oid relid, CmdType cmdType)
```

## Detailed Description
This function manages the table data structures used by PostgreSQL's deferred trigger system to track trigger-related information per relation and command type. It first searches through the existing table data structures in the current query's context to find a matching entry that is not marked as closed.

If no suitable existing structure is found, the function creates a new AfterTriggersTableData structure in the current transaction context. The function specifically avoids reusing structures marked as "closed" because they should not receive additional tuples, and their statement-level trigger firing state should not be modified.

The function operates within the current query depth context and ensures proper memory management by switching to CurTransactionContext when allocating new structures, which is appropriate since these structures don't need to persist beyond AfterTriggerEndQuery.

## Parameters / Member Variables
- `relid`: OID of the relation for which trigger table data is needed
- `cmdType`: Command type (INSERT, UPDATE, DELETE, etc.) that specifies the operation type for the trigger event

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - lappend
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - lfirst (macro)
  - foreach (macro)
- Called from (representative examples):
  - [MakeTransitionCaptureState](../M/MakeTransitionCaptureState.md)
  - before_stmt_triggers_fired
  - cancel_prior_stmt_triggers
  - [AfterTriggersTableData](../A/AfterTriggersTableData.md) (within trigger.c)

## Notes and Other Information
- Returns a pointer to either an existing or newly created AfterTriggersTableData structure
- Uses the global afterTriggers.query_stack to access the current query context
- Allocates new structures in CurTransactionContext for proper lifetime management
- Ensures query_depth bounds checking through assertions
- Ignores existing structures marked as "closed" to maintain proper trigger state isolation
- Part of PostgreSQL's trigger infrastructure for managing per-table, per-command trigger data during query execution
- The returned structure is used to track statement-level trigger firing state and associated tuple data