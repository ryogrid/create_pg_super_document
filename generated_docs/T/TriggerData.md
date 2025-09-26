# TriggerData

## Location
[src/include/commands/trigger.h:31-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/trigger.h#L31-L44)

## Overview
TriggerData is a structure that encapsulates all the context information needed when executing a trigger function, including the triggering event details, affected tuples, and transition tables.

## Definition

```c
typedef struct TriggerData
{
	NodeTag		type;
	TriggerEvent tg_event;
	Relation	tg_relation;
	HeapTuple	tg_trigtuple;
	HeapTuple	tg_newtuple;
	Trigger    *tg_trigger;
	TupleTableSlot *tg_trigslot;
	TupleTableSlot *tg_newslot;
	Tuplestorestate *tg_oldtable;
	Tuplestorestate *tg_newtable;
	const Bitmapset *tg_updatedcols;
} TriggerData;
```
## Detailed Description
The TriggerData structure serves as a comprehensive data container passed to trigger functions during their execution. It provides access to all relevant information about the triggering event, including the old and new tuple values, the relation being modified, and any transition tables that were requested. This structure enables trigger functions to examine the context of the operation that fired the trigger and make appropriate decisions or modifications.

The structure supports all trigger timing (BEFORE, AFTER, INSTEAD OF) and events (INSERT, UPDATE, DELETE, TRUNCATE), providing different subsets of information depending on the trigger type and timing.

## Parameters / Member Variables
- `type`: NodeTag identifier for memory management and type checking
- `tg_event`: Bit mask indicating the trigger event type (INSERT/UPDATE/DELETE/TRUNCATE) and timing (BEFORE/AFTER/INSTEAD OF)
- `tg_relation`: The relation (table) on which the trigger was fired
- `tg_trigtuple`: The tuple being inserted, updated, or deleted (NULL for TRUNCATE triggers)
- `tg_newtuple`: The new tuple version for UPDATE triggers (NULL for other trigger types)
- `*tg_trigger`: Pointer to the Trigger structure containing trigger metadata and configuration
- `*tg_trigslot`: TupleTableSlot containing the old tuple (alternative to tg_trigtuple for newer code)
- `*tg_newslot`: TupleTableSlot containing the new tuple (alternative to tg_newtuple for newer code)
- `*tg_oldtable`: Tuplestore containing all old tuple versions for statement-level triggers with OLD transition tables
- `*tg_newtable`: Tuplestore containing all new tuple versions for statement-level triggers with NEW transition tables
- `*tg_updatedcols`: Bitmapset indicating which columns were updated in an UPDATE trigger
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - TriggerEvent
  - [Relation](../R/Relation.md)
  - HeapTuple
  - [Trigger](Trigger.md)
  - [TupleTableSlot](TupleTableSlot.md)
  - [Tuplestorestate](Tuplestorestate.md)
  - [Bitmapset](../B/Bitmapset.md)

- Called from (representative examples):
  - [ExecCallTriggerFunc](../E/ExecCallTriggerFunc.md)
  - [ExecBSInsertTriggers](../E/ExecBSInsertTriggers.md)
  - [ExecBRInsertTriggers](../E/ExecBRInsertTriggers.md)
  - [ExecIRInsertTriggers](../E/ExecIRInsertTriggers.md)
  - [ExecBSDeleteTriggers](../E/ExecBSDeleteTriggers.md)
  - [ExecBRDeleteTriggersNew](../E/ExecBRDeleteTriggersNew.md)
  - [ExecIRDeleteTriggers](../E/ExecIRDeleteTriggers.md)
  - [ExecBSUpdateTriggers](../E/ExecBSUpdateTriggers.md)
  - [ExecBRUpdateTriggersNew](../E/ExecBRUpdateTriggersNew.md)
  - [ExecIRUpdateTriggers](../E/ExecIRUpdateTriggers.md)
  - [AfterTriggerExecute](../A/AfterTriggerExecute.md)
  - [RI_FKey_check_ins](../R/RI_FKey_check_ins.md)
  - [RI_FKey_check_upd](../R/RI_FKey_check_upd.md)
  - Various procedural language trigger handlers (plperl, plpython, pltcl)

## Notes and Other Information
- The CALLED_AS_TRIGGER macro can be used to check if a function is being called as a trigger by examining the TriggerData structure
- Different trigger types populate different subsets of the structure members (e.g., TRUNCATE triggers have NULL tg_trigtuple)
- The structure supports both row-level and statement-level triggers through different member combinations
- Transition tables (tg_oldtable, tg_newtable) are only available for statement-level AFTER triggers when explicitly requested in the trigger definition
- The tg_updatedcols bitmapset is only meaningful for UPDATE triggers and helps optimize trigger logic by identifying which columns actually changed