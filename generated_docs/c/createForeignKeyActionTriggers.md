# createForeignKeyActionTriggers

## Location
[src/backend/commands/tablecmds.c:12401-12535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L12401-L12535)

## Overview
Creates the referenced-side "action" triggers that implement foreign key constraints on the referenced table, handling ON DELETE and ON UPDATE actions.

## Definition

```c
static void
createForeignKeyActionTriggers(Relation rel, Oid refRelOid, Constraint *fkconstraint,
							   Oid constraintOid, Oid indexOid,
							   Oid parentDelTrigger, Oid parentUpdTrigger,
							   Oid *deleteTrigOid, Oid *updateTrigOid)
```
## Detailed Description
This function creates two constraint triggers on the referenced table to enforce foreign key actions when rows are deleted or updated. It builds and executes CREATE CONSTRAINT TRIGGER statements for both ON DELETE and ON UPDATE actions. The function supports all standard foreign key actions: NO ACTION, RESTRICT, CASCADE, SET NULL, and SET DEFAULT. Each action type is mapped to its corresponding referential integrity function (RI_FKey_*). The triggers are created as AFTER triggers that fire on row-level events.

## Parameters / Member Variables
- `rel`: The referencing relation (foreign key table)
- `refRelOid`: OID of the referenced relation (primary key table)
- `*fkconstraint`: Constraint definition containing FK actions and deferrability settings
- `constraintOid`: OID of the foreign key constraint
- `indexOid`: OID of the index supporting the foreign key
- `parentDelTrigger`: OID of parent delete trigger (for inheritance)
- `parentUpdTrigger`: OID of parent update trigger (for inheritance)
- `*deleteTrigOid`: Output parameter for created delete trigger OID
- `*updateTrigOid`: Output parameter for created update trigger OID
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (CreateTrigStmt creation)
  - SystemFuncName (RI function name generation)
  - [CreateTrigger](../C/CreateTrigger.md) (trigger creation)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (visibility control)
  - RelationGetRelid (relation OID extraction)
- Called from (representative examples):
  - child_dependency_type
  - [addFkRecurseReferenced](../a/addFkRecurseReferenced.md)

## Notes and Other Information
- Creates two separate triggers: one for DELETE events and one for UPDATE events
- Action triggers are deferrable only for NO ACTION constraints
- RESTRICT, CASCADE, SET NULL, and SET DEFAULT actions create non-deferrable triggers
- Uses CommandCounterIncrement() between trigger creations to ensure visibility
- Part of the foreign key constraint implementation infrastructure in PostgreSQL

## Simplified Source

```c
static void createForeignKeyActionTriggers(Relation rel, Oid refRelOid,
                                          Constraint *fkconstraint,
                                          Oid constraintOid, Oid indexOid,
                                          Oid parentDelTrigger, Oid parentUpdTrigger,
                                          Oid *deleteTrigOid, Oid *updateTrigOid) {
    CreateTrigStmt *fk_trigger;
    ObjectAddress trigAddress;

    // Create DELETE action trigger
    fk_trigger = makeNode(CreateTrigStmt);
    setup_trigger_common_properties(fk_trigger);
    fk_trigger->events = TRIGGER_TYPE_DELETE;

    // Set trigger function based on delete action type
    switch (fkconstraint->fk_del_action) {
        case FKCONSTR_ACTION_NOACTION:
            fk_trigger->funcname = SystemFuncName("RI_FKey_noaction_del");
            fk_trigger->deferrable = fkconstraint->deferrable;
            fk_trigger->initdeferred = fkconstraint->initdeferred;
            break;
        case FKCONSTR_ACTION_RESTRICT:
            fk_trigger->funcname = SystemFuncName("RI_FKey_restrict_del");
            break;
        case FKCONSTR_ACTION_CASCADE:
            fk_trigger->funcname = SystemFuncName("RI_FKey_cascade_del");
            break;
        case FKCONSTR_ACTION_SETNULL:
            fk_trigger->funcname = SystemFuncName("RI_FKey_setnull_del");
            break;
        case FKCONSTR_ACTION_SETDEFAULT:
            fk_trigger->funcname = SystemFuncName("RI_FKey_setdefault_del");
            break;
    }

    // Create the delete trigger
    trigAddress = CreateTrigger(fk_trigger, NULL, refRelOid,
                               RelationGetRelid(rel), constraintOid, indexOid,
                               InvalidOid, parentDelTrigger, NULL, true, false);
    if (deleteTrigOid)
        *deleteTrigOid = trigAddress.objectId;

    CommandCounterIncrement();

    // Create UPDATE action trigger (similar pattern)
    fk_trigger = makeNode(CreateTrigStmt);
    setup_trigger_common_properties(fk_trigger);
    fk_trigger->events = TRIGGER_TYPE_UPDATE;

    // Set trigger function based on update action type
    switch (fkconstraint->fk_upd_action) {
        // Similar switch structure for update actions...
    }

    // Create the update trigger
    trigAddress = CreateTrigger(fk_trigger, NULL, refRelOid,
                               RelationGetRelid(rel), constraintOid, indexOid,
                               InvalidOid, parentUpdTrigger, NULL, true, false);
    if (updateTrigOid)
        *updateTrigOid = trigAddress.objectId;
}
```