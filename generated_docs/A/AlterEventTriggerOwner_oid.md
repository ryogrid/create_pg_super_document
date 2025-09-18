# AlterEventTriggerOwner_oid

## Location
[src/backend/commands/event_trigger.c:510-534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L510-L534)

## Overview
Changes the owner of an event trigger identified by its OID, providing an alternative interface to AlterEventTriggerOwner for cases where the trigger's OID is already known.

## Definition


## Detailed Description
This function provides a streamlined interface for changing event trigger ownership when the caller already has the trigger's OID rather than its name. It follows the same basic pattern as AlterEventTriggerOwner but skips the name-to-OID lookup step since the OID is provided directly.

The function opens the pg_event_trigger system catalog, searches for the trigger using its OID, validates that it exists, and delegates the actual ownership change to AlterEventTriggerOwner_internal(). This function is typically used in scenarios like bulk ownership transfers or when working with dependency management where OIDs are the primary identifiers.

Unlike its name-based counterpart, this function returns void rather than an ObjectAddress since the caller presumably already has the necessary identification information.

## Parameters / Member Variables
- : OID of the event trigger whose ownership should be changed
- : OID of the user/role who should become the new owner of the event trigger

## Dependencies
- Functions called/Symbols referenced:
  - table_open (to open the pg_event_trigger relation)
  - SearchSysCacheCopy1 (to find the event trigger by OID)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (to convert trigger OID to Datum)
  - HeapTupleIsValid (to validate the found tuple)
  - ereport (to report errors if trigger not found)
  - [AlterEventTriggerOwner_internal](AlterEventTriggerOwner_internal.md) (to perform the actual ownership change)
  - [heap_freetuple](../h/heap_freetuple.md) (to free tuple memory)
  - table_close (to close the relation)
- Called from (representative examples):
  - [shdepReassignOwned_Owner](../s/shdepReassignOwned_Owner.md) (bulk ownership reassignment operations)

## Notes and Other Information
- This function does not return a value (void), unlike AlterEventTriggerOwner which returns ObjectAddress
- Used primarily in internal PostgreSQL operations like bulk ownership transfers and dependency management
- More efficient than the name-based version when the OID is already known, as it skips the name lookup step
- Error handling reports ERRCODE_UNDEFINED_OBJECT with the specific OID if the trigger doesn't exist
- The function requires an exclusive row lock on the pg_event_trigger relation to prevent concurrent modifications
- Memory management includes proper cleanup of the tuple copy obtained from the system cache
- Shares the core ownership change logic with AlterEventTriggerOwner through AlterEventTriggerOwner_internal()