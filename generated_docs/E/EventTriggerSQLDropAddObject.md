# EventTriggerSQLDropAddObject

## Location
[src/backend/commands/event_trigger.c:1278-1396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1278-L1396)

## Overview
Registers an object as being dropped by the current command, maintaining a list of dropped objects for event trigger processing.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's event trigger system for tracking dropped objects. It maintains a list of objects that have been dropped during the current command execution, which can later be consumed by event trigger functions via pg_event_trigger_dropped_objects().

The function performs several important tasks:
1. Validates that the object is supported by the event trigger system
2. Filters out temporary schemas that don't belong to the current session
3. Retrieves schema and object name information from the system catalogs
4. Handles temporary object identification 
5. Adds the object to the current event trigger state's SQLDropList

The function supports reentrancy, allowing event trigger functions to drop objects recursively while maintaining proper state management through a stack of EventTriggerQueryState structures.

## Parameters / Member Variables
- : Pointer to ObjectAddress structure identifying the dropped object (classId, objectId, objectSubId)
- : Boolean indicating if this is an original drop (true) or a cascaded drop (false)
- : Boolean indicating if this is a normal drop operation

## Dependencies
- Functions called/Symbols referenced:
  - [EventTriggerSupportsObject](EventTriggerSupportsObject.md) (validates object support)
  - [isAnyTempNamespace](../i/isAnyTempNamespace.md), isTempNamespace (temporary namespace checks)
  - [is_objectclass_supported](../i/is_objectclass_supported.md) (checks if object class supports schema qualification)
  - [get_catalog_object_by_oid](../g/get_catalog_object_by_oid.md) (retrieves catalog tuple)
  - [get_object_attnum_oid](../g/get_object_attnum_oid.md), get_object_attnum_namespace, get_object_attnum_name (attribute number retrieval)
  - [heap_getattr](../h/heap_getattr.md) (extracts attributes from heap tuples)
  - [get_namespace_name](../g/get_namespace_name.md) (retrieves namespace name)
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md) (gets object identity components)
  - getObjectTypeDescription (gets object type description)
  - [slist_push_head](../s/slist_push_head.md) (adds object to drop list)
- Called from (representative examples):
  - [deleteObjectsInList](../d/deleteObjectsInList.md) (src/backend/catalog/dependency.c:211)
  - [DropSubscription](../D/DropSubscription.md) (src/backend/commands/subscriptioncmds.c:1654)

## Notes and Other Information
- Only operates when currentEventTriggerState is active
- Filters out temporary schemas except those belonging to the current session
- Uses memory context switching to ensure proper memory management
- Creates SQLDropObject structures containing complete object information
- Temporary objects are marked with "pg_temp" schema name and istemp=true
- Located in src/backend/commands/event_trigger.c:1278-1396
- Essential for the event trigger infrastructure that supports DDL auditing and replication systems