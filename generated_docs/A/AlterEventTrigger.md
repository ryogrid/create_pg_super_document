# AlterEventTrigger

## Location
[src/backend/commands/event_trigger.c:423-474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L423-L474)

## Overview
Implements the ALTER EVENT TRIGGER command to enable, disable, or modify the firing mode of an existing event trigger in PostgreSQL.

## Definition

```c
Oid
AlterEventTrigger(AlterEventTrigStmt *stmt)
```
## Detailed Description
This function handles the SQL command "ALTER EVENT TRIGGER foo ENABLE|DISABLE|ENABLE ALWAYS|REPLICA" by modifying the evtenabled field in the pg_event_trigger system catalog. It provides a way to change the firing behavior of event triggers without dropping and recreating them.

The function performs several key operations: validates that the event trigger exists, checks that the current user has ownership permissions, updates the trigger's enabled status in the catalog, and handles special optimization for login event triggers. For login event triggers that are being enabled, it also calls SetDatabaseHasLoginEventTriggers() to set a database-level flag that optimizes login performance by indicating the presence of login triggers.

The function supports different enabling modes including completely disabled, enabled for normal operations, enabled always (even during recovery), and enabled only for replica operations.

## Parameters / Member Variables
- : Pointer to AlterEventTrigStmt structure containing:
  - : Name of the event trigger to alter
  - : New enabled status (TRIGGER_DISABLED, TRIGGER_FIRES_ON_ORIGIN, TRIGGER_FIRES_ALWAYS, or TRIGGER_FIRES_ON_REPLICA)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (to open the pg_event_trigger relation)
  - SearchSysCacheCopy1 (to find the event trigger by name)
  - [CStringGetDatum](../C/CStringGetDatum.md) (to convert trigger name to Datum)
  - HeapTupleIsValid (to validate the found tuple)
  - GETSTRUCT (to extract the form structure from the tuple)
  - [object_ownercheck](../o/object_ownercheck.md) (to verify ownership permissions)
  - [GetUserId](../G/GetUserId.md) (to get current user ID)
  - [aclcheck_error](../a/aclcheck_error.md) (to report permission errors)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (to update the catalog tuple)
  - [namestrcmp](../n/namestrcmp.md) (to compare event type names)
  - [SetDatabaseHasLoginEventTriggers](../S/SetDatabaseHasLoginEventTriggers.md) (to set database flag for login triggers)
  - InvokeObjectPostAlterHook (to invoke post-alter hooks)
  - [heap_freetuple](../h/heap_freetuple.md) (to free tuple memory)
  - [table_close](../t/table_close.md) (to close the relation)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (main utility command processor)

## Notes and Other Information
- Returns the OID of the altered event trigger
- Requires ownership of the event trigger to perform alterations
- Special handling for login event triggers to maintain the pg_database.dathasloginevt optimization flag
- The function operates on a copy of the tuple, allowing safe in-place modification
- Supports all standard trigger firing modes: disabled, normal, always, and replica-only
- Post-alter hooks are invoked to allow other subsystems to respond to the change
- Memory management includes proper cleanup of the tuple copy

## Simplified Source

```c
Oid AlterEventTrigger(AlterEventTrigStmt *stmt) {
    Relation tgrel;
    HeapTuple tup;
    Oid trigoid;
    Form_pg_event_trigger evtForm;
    char tgenabled = stmt->tgenabled;

    // Open pg_event_trigger catalog
    tgrel = table_open(EventTriggerRelationId, RowExclusiveLock);

    // Find the event trigger by name
    tup = SearchSysCacheCopy1(EVENTTRIGGERNAME, CStringGetDatum(stmt->trigname));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, "event trigger \"%s\" does not exist", stmt->trigname);

    evtForm = (Form_pg_event_trigger) GETSTRUCT(tup);
    trigoid = evtForm->oid;

    // Check ownership permissions
    if (!object_ownercheck(EventTriggerRelationId, trigoid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_EVENT_TRIGGER, stmt->trigname);

    // Update the enabled status
    evtForm->evtenabled = tgenabled;
    CatalogTupleUpdate(tgrel, &tup->t_self, tup);

    // Special handling for login event triggers
    if (namestrcmp(&evtForm->evtevent, "login") == 0 &&
        tgenabled != TRIGGER_DISABLED) {
        SetDatabaseHasLoginEventTriggers();
    }

    // Invoke post-alter hooks and cleanup
    InvokeObjectPostAlterHook(EventTriggerRelationId, trigoid, 0);
    heap_freetuple(tup);
    table_close(tgrel, RowExclusiveLock);

    return trigoid;
}
```