# EventTriggerSQLDropAddObject

## Location
[src/backend/commands/event_trigger.c:1278-1396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1278-L1396)

## Overview
Registers an object as being dropped by the current command, maintaining a list of dropped objects for event trigger processing.

## Definition

```c
void
EventTriggerSQLDropAddObject(const ObjectAddress *object, bool original, bool normal)
```
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
- `*object`: Pointer to ObjectAddress structure identifying the dropped object (classId, objectId, objectSubId)
- `original`: Boolean indicating if this is an original drop (true) or a cascaded drop (false)
- `normal`: Boolean indicating if this is a normal drop operation
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
  - [getObjectTypeDescription](../g/getObjectTypeDescription.md) (gets object type description)
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

## Simplified Source

```c
void EventTriggerSQLDropAddObject(const ObjectAddress *object, bool original, bool normal)
{
    SQLDropObject *obj;
    MemoryContext oldcxt;

    // Only process if event triggers are active
    if (!currentEventTriggerState)
        return;

    Assert(EventTriggerSupportsObject(object));

    // Skip temp schemas not owned by current session
    if (object->classId == NamespaceRelationId &&
        (isAnyTempNamespace(object->objectId) && !isTempNamespace(object->objectId)))
        return;

    // Switch to event trigger memory context
    oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

    // Create drop object record
    obj = palloc0(sizeof(SQLDropObject));
    obj->address = *object;
    obj->original = original;
    obj->normal = normal;

    // Get schema and object names from catalog if supported
    if (is_objectclass_supported(object->classId))
    {
        Relation catalog = table_open(obj->address.classId, AccessShareLock);
        HeapTuple tuple = get_catalog_object_by_oid(catalog,
                                                  get_object_attnum_oid(object->classId),
                                                  obj->address.objectId);
        if (tuple)
        {
            // Extract namespace information
            AttrNumber attnum = get_object_attnum_namespace(obj->address.classId);
            if (attnum != InvalidAttrNumber)
            {
                Datum datum;
                bool isnull;

                datum = heap_getattr(tuple, attnum, RelationGetDescr(catalog), &isnull);
                if (!isnull)
                {
                    Oid namespaceId = DatumGetObjectId(datum);
                    if (isTempNamespace(namespaceId))
                    {
                        obj->schemaname = "pg_temp";
                        obj->istemp = true;
                    }
                    else if (isAnyTempNamespace(namespaceId))
                    {
                        // Skip other session's temp objects
                        pfree(obj);
                        table_close(catalog, AccessShareLock);
                        MemoryContextSwitchTo(oldcxt);
                        return;
                    }
                    else
                    {
                        obj->schemaname = get_namespace_name(namespaceId);
                        obj->istemp = false;
                    }
                }
            }

            // Extract object name if unique within namespace
            if (get_object_namensp_unique(obj->address.classId) &&
                obj->address.objectSubId == 0)
            {
                attnum = get_object_attnum_name(obj->address.classId);
                if (attnum != InvalidAttrNumber)
                {
                    datum = heap_getattr(tuple, attnum, RelationGetDescr(catalog), &isnull);
                    if (!isnull)
                        obj->objname = pstrdup(NameStr(*DatumGetName(datum)));
                }
            }
        }
        table_close(catalog, AccessShareLock);
    }
    else
    {
        // Handle temp namespace case for unsupported classes
        if (object->classId == NamespaceRelationId && isTempNamespace(object->objectId))
            obj->istemp = true;
    }

    // Get object identity and type information
    obj->objidentity = getObjectIdentityParts(&obj->address, &obj->addrnames, &obj->addrargs, false);
    obj->objecttype = getObjectTypeDescription(&obj->address, false);

    // Add to drop list
    slist_push_head(&(currentEventTriggerState->SQLDropList), &obj->next);

    MemoryContextSwitchTo(oldcxt);
}
```