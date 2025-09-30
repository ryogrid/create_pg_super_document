# CreateFKCheckTrigger

## Location
[src/backend/commands/tablecmds.c:12338-12400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L12338-L12400)

## Overview
Creates the INSERT or UPDATE check trigger that enforces a foreign key constraint by calling appropriate referential integrity checking functions.

## Definition

```c
static Oid
CreateFKCheckTrigger(Oid myRelOid, Oid refRelOid, Constraint *fkconstraint,
					 Oid constraintOid, Oid indexOid, Oid parentTrigOid,
					 bool on_insert)
```
## Detailed Description
This function creates either an INSERT or UPDATE trigger that implements foreign key constraint checking on the referencing table. The trigger is configured to fire after the triggering event (INSERT or UPDATE) and calls the appropriate referential integrity function (RI_FKey_check_ins for inserts, RI_FKey_check_upd for updates). The function carefully manages trigger naming with a "RI_ConstraintTrigger_c_" prefix to ensure proper firing order relative to action triggers in self-referential foreign key scenarios.

The created trigger inherits deferability settings from the foreign key constraint and is automatically registered as a constraint trigger. The function ensures transaction visibility by calling CommandCounterIncrement() after trigger creation.

## Parameters / Member Variables
- : OID of the referencing table (where the foreign key column exists)
- : OID of the referenced table (where the primary key exists) 
- : The foreign key constraint definition containing deferability settings
- : OID of the constraint that this trigger implements
- : OID of the unique index supporting the referenced columns
- : OID of parent trigger for partitioned table inheritance
- : Boolean flag indicating whether to create INSERT trigger (true) or UPDATE trigger (false)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - SystemFuncName
  - [CreateTrigger](CreateTrigger.md)
  - [CommandCounterIncrement](CommandCounterIncrement.md)
  - TRIGGER_TYPE_INSERT
  - TRIGGER_TYPE_UPDATE
  - TRIGGER_TYPE_AFTER
- Called from (representative examples):
  - [createForeignKeyCheckTriggers](../c/createForeignKeyCheckTriggers.md)

## Notes and Other Information
- Uses "RI_ConstraintTrigger_c_" naming prefix to ensure proper trigger firing order
- Creates row-level AFTER triggers for referential integrity checking
- Inherits deferability characteristics from the parent foreign key constraint
- Automatically calls CommandCounterIncrement() to make trigger definition visible to subsequent operations
- Part of the foreign key constraint implementation infrastructure
- Returns the OID of the newly created trigger for further reference

## Simplified Source

```c
static Oid CreateFKCheckTrigger(Oid myRelOid, Oid refRelOid, Constraint *fkconstraint,
                               Oid constraintOid, Oid indexOid, Oid parentTrigOid,
                               bool on_insert) {
    // Create trigger statement structure
    CreateTrigStmt *fk_trigger = makeNode(CreateTrigStmt);
    fk_trigger->replace = false;
    fk_trigger->isconstraint = true;
    fk_trigger->trigname = "RI_ConstraintTrigger_c";  // 'c' for check triggers
    fk_trigger->relation = NULL;

    // Configure trigger function and event type
    if (on_insert) {
        fk_trigger->funcname = SystemFuncName("RI_FKey_check_ins");
        fk_trigger->events = TRIGGER_TYPE_INSERT;
    } else {
        fk_trigger->funcname = SystemFuncName("RI_FKey_check_upd");
        fk_trigger->events = TRIGGER_TYPE_UPDATE;
    }

    // Set trigger properties
    fk_trigger->args = NIL;
    fk_trigger->row = true;           // Row-level trigger
    fk_trigger->timing = TRIGGER_TYPE_AFTER;
    fk_trigger->columns = NIL;
    fk_trigger->whenClause = NULL;
    fk_trigger->transitionRels = NIL;

    // Inherit constraint properties
    fk_trigger->deferrable = fkconstraint->deferrable;
    fk_trigger->initdeferred = fkconstraint->initdeferred;
    fk_trigger->constrrel = NULL;

    // Create the actual trigger
    ObjectAddress trigAddress = CreateTrigger(fk_trigger, NULL, myRelOid, refRelOid,
                                            constraintOid, indexOid, InvalidOid,
                                            parentTrigOid, NULL, true, false);

    // Make trigger visible to subsequent operations
    CommandCounterIncrement();

    return trigAddress.objectId;
}
```