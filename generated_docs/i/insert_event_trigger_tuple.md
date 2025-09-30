# insert_event_trigger_tuple

## Location
[src/backend/commands/event_trigger.c:273-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L273-L355)

## Overview
Inserts a new event trigger tuple into the pg_event_trigger system catalog and records all necessary dependencies for the trigger.

## Definition
```c
static Oid insert_event_trigger_tuple(const char *trigname, const char *eventname, Oid evtOwner, Oid funcoid, List *taglist)
```

## Detailed Description
insert_event_trigger_tuple is the core function responsible for physically creating the event trigger entry in PostgreSQL's system catalogs. It constructs a tuple for the pg_event_trigger catalog with all the necessary attributes including trigger name, event name, owner, function OID, enabled status, and tag filters. The function handles the complete lifecycle of catalog insertion including acquiring locks, generating a new OID, building the tuple data, inserting it into the catalog, recording dependencies, and invoking post-creation hooks. For login event triggers, it also sets a database-level flag for performance optimization.

## Parameters / Member Variables
- `trigname`: The name of the event trigger being created
- `eventname`: The event type (ddl_command_start, ddl_command_end, sql_drop, login, table_rewrite)
- `evtOwner`: The OID of the user who owns the event trigger
- `funcoid`: The OID of the function that will be called when the event trigger fires
- `taglist`: A List of command tags to filter on, or NIL if no filtering

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)() - opens the pg_event_trigger relation with lock
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)() - generates a new unique OID for the trigger
  - [namestrcpy](../n/namestrcpy.md)() - copies strings into NameData structures
  - [NameGetDatum](../N/NameGetDatum.md)() - converts NameData to Datum
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)() - converts OID to Datum
  - [CharGetDatum](../C/CharGetDatum.md)() - converts char to Datum
  - [filter_list_to_array](../f/filter_list_to_array.md)() - converts tag list to array for storage
  - [heap_form_tuple](../h/heap_form_tuple.md)() - creates a heap tuple from values and nulls arrays
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)() - inserts the tuple into the catalog
  - [heap_freetuple](../h/heap_freetuple.md)() - frees the temporary heap tuple
  - [SetDatabaseHasLoginEventTriggers](../S/SetDatabaseHasLoginEventTriggers.md)() - sets database flag for login triggers
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)() - records ownership dependency
  - [recordDependencyOn](../r/recordDependencyOn.md)() - records function dependency
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)() - records extension dependency if applicable
  - InvokeObjectPostCreateHook() - invokes post-creation hooks
  - [table_close](../t/table_close.md)() - closes the relation and releases lock
- Called from (representative examples):
  - [CreateEventTrigger](../C/CreateEventTrigger.md)() - as the final step in event trigger creation

## Notes and Other Information
- This is a static function only accessible within event_trigger.c
- Uses RowExclusiveLock on pg_event_trigger to ensure safe concurrent access
- Generates a new OID using the EventTriggerOidIndexId unique index
- Sets the trigger to TRIGGER_FIRES_ON_ORIGIN by default (enabled)
- For login event triggers, optimizes future lookups by setting a database-level flag
- Records three types of dependencies: owner dependency, function dependency, and extension dependency
- Handles NULL tag lists properly by setting the appropriate null flag
- Returns the newly assigned OID for the event trigger
- Part of PostgreSQL's dependency tracking system to ensure proper cleanup during DROP operations
- The function is transactional - if any step fails, the entire operation will be rolled back

## Simplified Source

```c
static Oid insert_event_trigger_tuple(const char *trigname, const char *eventname,
                                     Oid evtOwner, Oid funcoid, List *taglist) {
    Relation tgrel;
    Oid trigoid;
    HeapTuple tuple;
    Datum values[Natts_pg_trigger];
    bool nulls[Natts_pg_trigger];
    NameData evtnamedata, evteventdata;
    ObjectAddress myself, referenced;

    // Open pg_event_trigger catalog table
    tgrel = table_open(EventTriggerRelationId, RowExclusiveLock);

    // Generate new OID and build tuple values
    trigoid = GetNewOidWithIndex(tgrel, EventTriggerOidIndexId, Anum_pg_event_trigger_oid);
    values[Anum_pg_event_trigger_oid - 1] = ObjectIdGetDatum(trigoid);
    memset(nulls, false, sizeof(nulls));

    // Set trigger name and event name
    namestrcpy(&evtnamedata, trigname);
    values[Anum_pg_event_trigger_evtname - 1] = NameGetDatum(&evtnamedata);
    namestrcpy(&evteventdata, eventname);
    values[Anum_pg_event_trigger_evtevent - 1] = NameGetDatum(&evteventdata);

    // Set owner, function, and enabled status
    values[Anum_pg_event_trigger_evtowner - 1] = ObjectIdGetDatum(evtOwner);
    values[Anum_pg_event_trigger_evtfoid - 1] = ObjectIdGetDatum(funcoid);
    values[Anum_pg_event_trigger_evtenabled - 1] = CharGetDatum(TRIGGER_FIRES_ON_ORIGIN);

    // Handle tag filtering
    if (taglist == NIL)
        nulls[Anum_pg_event_trigger_evttags - 1] = true;
    else
        values[Anum_pg_event_trigger_evttags - 1] = filter_list_to_array(taglist);

    // Insert the tuple into catalog
    tuple = heap_form_tuple(tgrel->rd_att, values, nulls);
    CatalogTupleInsert(tgrel, tuple);
    heap_freetuple(tuple);

    // Special handling for login event triggers
    if (strcmp(eventname, "login") == 0)
        SetDatabaseHasLoginEventTriggers();

    // Record dependencies
    recordDependencyOnOwner(EventTriggerRelationId, trigoid, evtOwner);

    myself.classId = EventTriggerRelationId;
    myself.objectId = trigoid;
    myself.objectSubId = 0;
    referenced.classId = ProcedureRelationId;
    referenced.objectId = funcoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    recordDependencyOnCurrentExtension(&myself, false);

    // Invoke post-creation hooks and cleanup
    InvokeObjectPostCreateHook(EventTriggerRelationId, trigoid, 0);
    table_close(tgrel, RowExclusiveLock);

    return trigoid;
}
```