# CreateEventTrigger

## Location
[src/backend/commands/event_trigger.c:120-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L120-L211)

## Overview
Creates a new event trigger in the PostgreSQL database, handling validation, permission checks, and catalog insertion for triggers that fire on specific database events.

## Definition

```c
Oid
CreateEventTrigger(CreateEventTrigStmt *stmt)
```
## Detailed Description
CreateEventTrigger is the main function responsible for creating event triggers in PostgreSQL. Event triggers are special triggers that fire on DDL events (like CREATE, ALTER, DROP commands), login events, or table rewrite operations across the entire database rather than on specific tables. The function performs comprehensive validation including superuser privilege checks, event name validation, filter condition parsing, tag validation, function signature verification, and prevents duplicate trigger names before inserting the new trigger into the system catalogs.

## Parameters / Member Variables
- `*stmt`: A CreateEventTrigStmt structure containing all the information needed to create the event trigger, including trigger name, event name, function name, and filter conditions (WHEN clauses)
## Dependencies
- Functions called/Symbols referenced:
  - [superuser](../s/superuser.md)() - checks if current user has superuser privileges
  - [error_duplicate_filter_variable](../e/error_duplicate_filter_variable.md)() - reports error for duplicate filter variables
  - [validate_ddl_tags](../v/validate_ddl_tags.md)() - validates tag filters for DDL events
  - [validate_table_rewrite_tags](../v/validate_table_rewrite_tags.md)() - validates tag filters for table rewrite events
  - [SearchSysCache1](../S/SearchSysCache1.md)() - searches system catalog for existing triggers
  - [LookupFuncName](../L/LookupFuncName.md)() - looks up the trigger function
  - [get_func_rettype](../g/get_func_rettype.md)() - gets the return type of the function
  - [insert_event_trigger_tuple](../i/insert_event_trigger_tuple.md)() - inserts the new trigger into catalogs
  - [CStringGetDatum](CStringGetDatum.md)() - converts C string to Datum
  - [NameListToString](../N/NameListToString.md)() - converts function name list to string
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)() - [main](../m/main.md) utility command processing function

## Notes and Other Information
- Requires superuser privileges to create event triggers due to privilege escalation risks
- Supports five event types: ddl_command_start, ddl_command_end, sql_drop, login, and table_rewrite
- Tag filtering is supported for DDL and table rewrite events but not for login events
- The trigger function must return type 'event_trigger'
- Prevents creation of duplicate event triggers with the same name
- Returns the OID of the newly created event trigger
- Part of PostgreSQL's event trigger system introduced to provide hooks for DDL auditing and replication tools

## Simplified Source

```c
Oid CreateEventTrigger(CreateEventTrigStmt *stmt) {
    HeapTuple tuple;
    Oid funcoid;
    Oid funcrettype;
    Oid evtowner = GetUserId();
    List *tags = NULL;

    // Must be superuser to create event triggers
    if (!superuser()) {
        ereport(ERROR, "permission denied to create event trigger \"%s\"",
                stmt->trigname);
    }

    // Validate event name
    if (strcmp(stmt->eventname, "ddl_command_start") != 0 &&
        strcmp(stmt->eventname, "ddl_command_end") != 0 &&
        strcmp(stmt->eventname, "sql_drop") != 0 &&
        strcmp(stmt->eventname, "login") != 0 &&
        strcmp(stmt->eventname, "table_rewrite") != 0) {
        ereport(ERROR, "unrecognized event name \"%s\"", stmt->eventname);
    }

    // Process filter conditions (WHEN clauses)
    foreach(lc, stmt->whenclause) {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "tag") == 0) {
            if (tags != NULL)
                error_duplicate_filter_variable(def->defname);
            tags = (List *) def->arg;
        } else {
            ereport(ERROR, "unrecognized filter variable \"%s\"", def->defname);
        }
    }

    // Validate tag filters based on event type
    if ((strcmp(stmt->eventname, "ddl_command_start") == 0 ||
         strcmp(stmt->eventname, "ddl_command_end") == 0 ||
         strcmp(stmt->eventname, "sql_drop") == 0) && tags != NULL) {
        validate_ddl_tags("tag", tags);
    } else if (strcmp(stmt->eventname, "table_rewrite") == 0 && tags != NULL) {
        validate_table_rewrite_tags("tag", tags);
    } else if (strcmp(stmt->eventname, "login") == 0 && tags != NULL) {
        ereport(ERROR, "tag filtering not supported for login event triggers");
    }

    // Check for duplicate trigger name
    tuple = SearchSysCache1(EVENTTRIGGERNAME, CStringGetDatum(stmt->trigname));
    if (HeapTupleIsValid(tuple)) {
        ereport(ERROR, "event trigger \"%s\" already exists", stmt->trigname);
    }

    // Validate trigger function
    funcoid = LookupFuncName(stmt->funcname, 0, NULL, false);
    funcrettype = get_func_rettype(funcoid);
    if (funcrettype != EVENT_TRIGGEROID) {
        ereport(ERROR, "function %s must return type %s",
                NameListToString(stmt->funcname), "event_trigger");
    }

    // Insert the new event trigger
    return insert_event_trigger_tuple(stmt->trigname, stmt->eventname,
                                     evtowner, funcoid, tags);
}
```