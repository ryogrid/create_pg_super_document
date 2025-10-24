# getEventTriggers

## Location
[src/bin/pg_dump/pg_dump.c:8421-8507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8421-L8507)

## Overview
Retrieves information about all PostgreSQL event triggers from the system catalog, supporting databases running PostgreSQL 9.3 and later versions.

## Definition

```c
EventTriggerInfo *
getEventTriggers(Archive *fout, int *numEventTriggers)
```
## Detailed Description
This function queries the pg_event_trigger system catalog to collect comprehensive information about all event triggers defined in the database. Event triggers are a PostgreSQL feature introduced in version 9.3 that allow functions to be executed in response to DDL events across the entire database.

The function handles version compatibility by returning NULL for PostgreSQL versions prior to 9.3, where event triggers did not exist. For supported versions, it extracts detailed information about each event trigger, including the trigger name, triggering event, owner, associated tags, and the function to be executed.

The query uses several PostgreSQL-specific features:
- Converts the evttags array to a formatted string representation
- Uses the regproc type cast to get human-readable function names
- Processes all fields necessary for recreating the event trigger during restoration

Each event trigger is processed through the standard dumpable object system, allowing for selective dumping based on user preferences and dump scope.

## Parameters / Member Variables
- `*fout`: Archive pointer containing database connection and version information
- `*numEventTriggers`: Output parameter that receives the total number of event triggers found
## Dependencies
- Functions called/Symbols referenced:
  - EventTriggerInfo (struct type)
  - [createPQExpBuffer](../c/createPQExpBuffer.md), appendPQExpBufferStr (query building)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (query execution)
  - [PQntuples](../P/PQntuples.md), PQfnumber, PQgetvalue (libpq result processing functions)
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
  - atooid (OID conversion)
  - [AssignDumpId](../A/AssignDumpId.md) (dump ID assignment)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
  - [getRoleName](getRoleName.md) (role name resolution)
  - [selectDumpableObject](../s/selectDumpableObject.md) (dump selection logic)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md) (cleanup)
  - DO_EVENT_TRIGGER (object type enum)
  - PGRES_TUPLES_OK (result status)

- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) (primary caller during schema data collection phase)

## Notes and Other Information
- Returns NULL for PostgreSQL versions before 9.3, where event triggers were not supported
- Returns a dynamically allocated array of EventTriggerInfo structures that the caller must manage
- Event triggers operate at the database level, unlike regular triggers which are table-specific
- The evttags field is converted from a PostgreSQL array to a comma-separated string for easier handling
- Uses regproc casting to convert function OIDs to readable function names in the dump
- All event triggers go through the selectDumpableObject filtering process
- The function processes event triggers in OID order for consistent dump output
- Event triggers can be filtered by events (DDL command start, DDL command end, SQL drop, table rewrite)
- Tags allow event triggers to be more selective about which DDL commands they respond to

## Simplified Source

```c
EventTriggerInfo *
getEventTriggers(Archive *fout, int *numEventTriggers)
{
    PQExpBuffer query;
    PGresult   *res;
    EventTriggerInfo *evtinfo;
    int         ntups;

    // Check version compatibility - event triggers introduced in 9.3
    if (fout->remoteVersion < 90300) {
        *numEventTriggers = 0;
        return NULL;
    }

    // Build query to retrieve event trigger information
    query = createPQExpBuffer();
    appendPQExpBufferStr(query,
                         "SELECT e.tableoid, e.oid, evtname, evtenabled, "
                         "evtevent, evtowner, "
                         "array_to_string(array("
                         "select quote_literal(x) "
                         " from unnest(evttags) as t(x)), ', ') as evttags, "
                         "e.evtfoid::regproc as evtfname "
                         "FROM pg_event_trigger e "
                         "ORDER BY e.oid");

    // Execute query and get results
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numEventTriggers = ntups;

    // Allocate memory for event trigger info array
    evtinfo = (EventTriggerInfo *) pg_malloc(ntups * sizeof(EventTriggerInfo));

    // Get column indices for result processing
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_evtname = PQfnumber(res, "evtname");
    int i_evtevent = PQfnumber(res, "evtevent");
    int i_evtowner = PQfnumber(res, "evtowner");
    int i_evttags = PQfnumber(res, "evttags");
    int i_evtfname = PQfnumber(res, "evtfname");
    int i_evtenabled = PQfnumber(res, "evtenabled");

    // Process each event trigger result
    for (int i = 0; i < ntups; i++) {
        // Initialize dump object metadata
        evtinfo[i].dobj.objType = DO_EVENT_TRIGGER;
        evtinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        evtinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&evtinfo[i].dobj);

        // Copy event trigger properties
        evtinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_evtname));
        evtinfo[i].evtname = pg_strdup(PQgetvalue(res, i, i_evtname));
        evtinfo[i].evtevent = pg_strdup(PQgetvalue(res, i, i_evtevent));
        evtinfo[i].evtowner = getRoleName(PQgetvalue(res, i, i_evtowner));
        evtinfo[i].evttags = pg_strdup(PQgetvalue(res, i, i_evttags));
        evtinfo[i].evtfname = pg_strdup(PQgetvalue(res, i, i_evtfname));
        evtinfo[i].evtenabled = *(PQgetvalue(res, i, i_evtenabled));

        // Determine if this trigger should be dumped
        selectDumpableObject(&(evtinfo[i].dobj), fout);
    }

    // Cleanup and return results
    PQclear(res);
    destroyPQExpBuffer(query);
    return evtinfo;
}
```