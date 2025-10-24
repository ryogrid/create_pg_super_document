# pg_event_trigger_table_rewrite_oid

## Location
[src/backend/commands/event_trigger.c:1493-1513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1493-L1513)

## Overview
A PostgreSQL built-in function that returns the OID of the table being rewritten, available only within table_rewrite event trigger functions.

## Definition

```c
struct CollectedCommand representation of itself to the command list,
 * using the routines below.
 *
 * 2) Some time after that, ddl_command_end fires and the command list is made
 * available to the event trigger function via pg_event_trigger_ddl_commands();
```
## Detailed Description
This function provides access to the Object Identifier (OID) of the table that is currently being rewritten during a table rewrite operation. It can only be called from within table_rewrite event trigger functions and serves as a way for event trigger functions to identify which specific table is undergoing the rewrite operation.

The function enforces strict calling context validation to ensure it's used only when table rewrite information is available and meaningful. It checks that there is an active event trigger state and that the table_rewrite_oid field contains a valid OID (not InvalidOid). If called outside the proper context, it raises a protocol violation error.

Table rewrites occur during various DDL operations such as ALTER TABLE commands that require the table data to be physically reorganized, type changes that require data conversion, or adding columns with non-null defaults to large tables.

## Parameters / Member Variables
- Returns: OID of the table being rewritten

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_OID (macro for returning OID values)
  - ereport, errcode, errmsg (error reporting functions)
  - ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED (error code constant)
- Called from:
  - No direct references (invoked by SQL as built-in function)

## Notes and Other Information
- Accessible only within table_rewrite event trigger functions - raises protocol violation error if called elsewhere
- Returns the table_rewrite_oid field from the current event trigger state
- Provides essential table identification for event triggers monitoring table rewrite operations
- Useful for logging, auditing, or implementing custom logic during table rewrites
- The OID returned corresponds to the pg_class entry for the table being rewritten
- Located in src/backend/commands/event_trigger.c:1493-1513
- Simple function with focused responsibility - just validates context and returns the stored OID

## Simplified Source

```c
Datum pg_event_trigger_table_rewrite_oid(PG_FUNCTION_ARGS) {
    // Validate we're in a table rewrite event trigger context
    if (!currentEventTriggerState ||
        currentEventTriggerState->table_rewrite_oid == InvalidOid) {
        ereport(ERROR, "Function can only be called in table_rewrite event trigger");
    }

    // Return the OID of the table being rewritten
    PG_RETURN_OID(currentEventTriggerState->table_rewrite_oid);
}
```