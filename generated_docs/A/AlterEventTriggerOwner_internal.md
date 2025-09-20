# AlterEventTriggerOwner_internal

## Location
[src/backend/commands/event_trigger.c:535-574](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L535-L574)

## Overview
Internal workhorse function that handles the core logic for changing an event trigger's owner, including permission checks and catalog updates.

## Definition

```c
static void
AlterEventTriggerOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
```
## Detailed Description
This function performs the actual work of changing an event trigger's owner. It operates on an already-retrieved tuple from the pg_event_trigger catalog and handles all the necessary validation, permission checking, and catalog updates. The function ensures that only superusers can own event triggers and that the current user has permission to change ownership. It updates both the catalog tuple and the dependency system to reflect the ownership change.

## Parameters / Member Variables
- : Open relation handle to the pg_event_trigger catalog table
- : HeapTuple containing the event trigger record to be modified
- : OID of the new owner (must be a superuser)

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_event_trigger (catalog form structure)
  - [object_ownercheck](../o/object_ownercheck.md) (permission validation)
  - [aclcheck_error](../a/aclcheck_error.md) (access control error reporting)
  - superuser_arg (superuser privilege check)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog tuple modification)
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md) (dependency system update)
  - InvokeObjectPostAlterHook (post-alter event notification)
- Called from (representative examples):
  - [AlterEventTriggerOwner](AlterEventTriggerOwner.md)
  - [AlterEventTriggerOwner_oid](AlterEventTriggerOwner_oid.md)

## Notes and Other Information
- This is a static internal function, not exposed outside event_trigger.c
- Enforces the PostgreSQL security model that only superusers can own event triggers
- Returns early if the new owner is the same as the current owner (no-op case)
- Updates both the catalog record and the dependency tracking system atomically
- Triggers post-alter hooks to notify other subsystems of the ownership change
- Part of the event trigger ownership management subsystem in PostgreSQL