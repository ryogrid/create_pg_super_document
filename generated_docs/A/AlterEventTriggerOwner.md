# AlterEventTriggerOwner

## Location
[src/backend/commands/event_trigger.c:475-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L475-L509)

## Overview
Changes the owner of an event trigger identified by name, implementing the ALTER EVENT TRIGGER ... OWNER TO command functionality.

## Definition


## Detailed Description
This function handles changing the ownership of an event trigger by looking up the trigger by name and delegating the actual ownership change to AlterEventTriggerOwner_internal(). It serves as the main entry point for the ALTER EVENT TRIGGER ... OWNER TO SQL command when the trigger is specified by name rather than OID.

The function performs the standard pattern for PostgreSQL object ownership changes: opens the relevant system catalog with exclusive row lock, searches for the object by name, validates its existence, performs the ownership change through the internal function, constructs and returns an ObjectAddress for the modified object, and cleans up resources.

## Parameters / Member Variables
- : C string containing the name of the event trigger whose ownership should be changed
- : OID of the user/role who should become the new owner of the event trigger

## Dependencies
- Functions called/Symbols referenced:
  - table_open (to open the pg_event_trigger relation)
  - SearchSysCacheCopy1 (to find the event trigger by name)
  - [CStringGetDatum](../C/CStringGetDatum.md) (to convert trigger name to Datum)
  - HeapTupleIsValid (to validate the found tuple)
  - ereport (to report errors if trigger not found)
  - GETSTRUCT (to extract the form structure from the tuple)
  - [AlterEventTriggerOwner_internal](AlterEventTriggerOwner_internal.md) (to perform the actual ownership change)
  - ObjectAddressSet (to construct the return ObjectAddress)
  - [heap_freetuple](../h/heap_freetuple.md) (to free tuple memory)
  - table_close (to close the relation)
- Called from (representative examples):
  - [ExecAlterOwnerStmt](../E/ExecAlterOwnerStmt.md) (main ALTER OWNER command processor)

## Notes and Other Information
- Returns an ObjectAddress structure containing the EventTriggerRelationId and the OID of the altered trigger
- The function requires an exclusive row lock on the pg_event_trigger relation to prevent concurrent modifications
- Error handling includes reporting ERRCODE_UNDEFINED_OBJECT if the named trigger doesn't exist
- The actual ownership change logic is delegated to AlterEventTriggerOwner_internal() for code reuse
- Memory management includes proper cleanup of the tuple copy obtained from the system cache
- This function is typically called as part of the SQL command processing pipeline for ALTER EVENT TRIGGER ... OWNER TO statements