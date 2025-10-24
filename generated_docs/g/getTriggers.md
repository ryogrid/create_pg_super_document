# getTriggers

## Location
[src/bin/pg_dump/pg_dump.c:8225-8420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8225-L8420)

## Overview
Retrieves comprehensive trigger information for all dumpable tables from the PostgreSQL system catalog, handling version-specific logic and partitioned table triggers.

## Definition

```c
void
getTriggers(Archive *fout, TableInfo tblinfo[], int numTables)
```
## Detailed Description
This function performs a sophisticated query against the pg_trigger system catalog to collect information about triggers on tables that need to be dumped. It implements version-specific logic to handle differences in PostgreSQL's trigger system across major versions, particularly around partitioned tables and inherited triggers.

The function uses an optimized approach where it builds a constraint list of table OIDs to avoid selecting all triggers system-wide. This is both a security measure (avoiding functions on tables without locks) and a performance optimization. It handles several complex scenarios:

- **Version 15+**: Uses tgparentid to identify partition triggers and checks for enabled state differences
- **Version 13-14**: Uses tgisinternal flag and tgparentid for partition trigger handling
- **Version 11-12**: Uses pg_depend to match partition triggers since tgparentid doesn't exist
- **Earlier versions**: Simple trigger collection without partition support

The function also ensures that partition triggers are included when their enabled state differs from their parent trigger, allowing for proper restoration of trigger state variations across partition hierarchies.

## Parameters / Member Variables
- `*fout`: Archive pointer containing database connection information and version details
- `tblinfo[]`: Array of TableInfo structures representing tables to process
- `numTables`: Number of elements in the tblinfo array
## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md), TriggerInfo (struct types)
  - [createPQExpBuffer](../c/createPQExpBuffer.md), appendPQExpBufferChar, appendPQExpBuffer (query building)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (query execution)
  - [PQntuples](../P/PQntuples.md), PQfnumber, PQgetvalue (libpq result processing)
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
  - atooid (OID conversion)
  - [AssignDumpId](../A/AssignDumpId.md) (dump ID assignment)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md) (cleanup)
  - DUMP_COMPONENT_DEFINITION (dump component flag)
  - DO_TRIGGER (object type enum)
  - PGRES_TUPLES_OK (result status)

- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) (primary caller during schema data collection)

## Notes and Other Information
- The function does not return trigger data directly; instead, it populates the triggers and numTriggers fields in the corresponding TableInfo structures
- Implements sophisticated version-specific SQL queries to handle PostgreSQL's evolving partition trigger system
- Uses pg_get_triggerdef with pretty=false to ensure forward-compatible dump output
- Handles both regular triggers and partition-specific triggers with different enabled states
- Includes optimization to process only tables that actually have triggers (hastriggers flag)
- The function assumes tblinfo array is sorted by OID for efficient table lookup
- Partition triggers are included even if marked as internal when their enabled state differs from the parent
- Memory allocation creates a single array for all triggers, with per-table pointers into this array

## Simplified Source

```c
void getTriggers(Archive *fout, TableInfo tblinfo[], int numTables)
{
    PQExpBuffer query = createPQExpBuffer();
    PQExpBuffer tbloids = createPQExpBuffer();
    PGresult   *res;
    int         ntups;
    TriggerInfo *tginfo;

    // Build array of table OIDs that have triggers and need dumping
    appendPQExpBufferChar(tbloids, '{');
    for (int i = 0; i < numTables; i++)
    {
        TableInfo *tbinfo = &tblinfo[i];

        if (!tbinfo->hastriggers ||
            !(tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
            continue;

        if (tbloids->len > 1)
            appendPQExpBufferChar(tbloids, ',');
        appendPQExpBuffer(tbloids, "%u", tbinfo->dobj.catId.oid);
    }
    appendPQExpBufferChar(tbloids, '}');

    // Build version-specific query for trigger information
    if (fout->remoteVersion >= 150000)
    {
        // v15+: Use tgparentid for partition trigger detection
        appendPQExpBuffer(query,
            "SELECT t.tgrelid, t.tgname, "
            "pg_catalog.pg_get_triggerdef(t.oid, false) AS tgdef, "
            "t.tgenabled, t.tableoid, t.oid, "
            "t.tgparentid <> 0 AS tgispartition "
            "FROM unnest('%s'::pg_catalog.oid[]) AS src(tbloid) "
            "JOIN pg_catalog.pg_trigger t ON (src.tbloid = t.tgrelid) "
            "LEFT JOIN pg_catalog.pg_trigger u ON (u.oid = t.tgparentid) "
            "WHERE ((NOT t.tgisinternal AND t.tgparentid = 0) "
            "OR t.tgenabled != u.tgenabled) "
            "ORDER BY t.tgrelid, t.tgname",
            tbloids->data);
    }
    else if (fout->remoteVersion >= 130000)
    {
        // v13-14: Use tgisinternal and tgparentid
        appendPQExpBuffer(query,
            "SELECT t.tgrelid, t.tgname, "
            "pg_catalog.pg_get_triggerdef(t.oid, false) AS tgdef, "
            "t.tgenabled, t.tableoid, t.oid, t.tgisinternal as tgispartition "
            "FROM unnest('%s'::pg_catalog.oid[]) AS src(tbloid) "
            "JOIN pg_catalog.pg_trigger t ON (src.tbloid = t.tgrelid) "
            "LEFT JOIN pg_catalog.pg_trigger u ON (u.oid = t.tgparentid) "
            "WHERE (NOT t.tgisinternal OR t.tgenabled != u.tgenabled) "
            "ORDER BY t.tgrelid, t.tgname",
            tbloids->data);
    }
    else if (fout->remoteVersion >= 110000)
    {
        // v11-12: Use pg_depend to match partition triggers
        appendPQExpBuffer(query,
            "SELECT t.tgrelid, t.tgname, "
            "pg_catalog.pg_get_triggerdef(t.oid, false) AS tgdef, "
            "t.tgenabled, t.tableoid, t.oid, t.tgisinternal as tgispartition "
            "FROM unnest('%s'::pg_catalog.oid[]) AS src(tbloid) "
            "JOIN pg_catalog.pg_trigger t ON (src.tbloid = t.tgrelid) "
            "LEFT JOIN pg_catalog.pg_depend AS d ON (...) "
            "LEFT JOIN pg_catalog.pg_trigger AS pt ON pt.oid = refobjid "
            "WHERE (NOT t.tgisinternal OR t.tgenabled != pt.tgenabled) "
            "ORDER BY t.tgrelid, t.tgname",
            tbloids->data);
    }
    else
    {
        // Earlier versions: Simple trigger collection
        appendPQExpBuffer(query,
            "SELECT t.tgrelid, t.tgname, "
            "pg_catalog.pg_get_triggerdef(t.oid, false) AS tgdef, "
            "t.tgenabled, false as tgispartition, t.tableoid, t.oid "
            "FROM unnest('%s'::pg_catalog.oid[]) AS src(tbloid) "
            "JOIN pg_catalog.pg_trigger t ON (src.tbloid = t.tgrelid) "
            "WHERE NOT tgisinternal ORDER BY t.tgrelid, t.tgname",
            tbloids->data);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Extract column indices
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_tgrelid = PQfnumber(res, "tgrelid");
    int i_tgname = PQfnumber(res, "tgname");
    int i_tgenabled = PQfnumber(res, "tgenabled");
    int i_tgispartition = PQfnumber(res, "tgispartition");
    int i_tgdef = PQfnumber(res, "tgdef");

    tginfo = (TriggerInfo *) pg_malloc(ntups * sizeof(TriggerInfo));

    // Process results grouped by table
    int curtblindx = -1;
    for (int j = 0; j < ntups;)
    {
        Oid tgrelid = atooid(PQgetvalue(res, j, i_tgrelid));
        TableInfo *tbinfo = NULL;
        int numtrigs;

        // Count triggers for this table
        for (numtrigs = 1; numtrigs < ntups - j; numtrigs++)
            if (atooid(PQgetvalue(res, j + numtrigs, i_tgrelid)) != tgrelid)
                break;

        // Find corresponding TableInfo
        while (++curtblindx < numTables)
        {
            tbinfo = &tblinfo[curtblindx];
            if (tbinfo->dobj.catId.oid == tgrelid)
                break;
        }

        // Store trigger array reference in table
        tbinfo->triggers = tginfo + j;
        tbinfo->numTriggers = numtrigs;

        // Process each trigger for this table
        for (int c = 0; c < numtrigs; c++, j++)
        {
            tginfo[j].dobj.objType = DO_TRIGGER;
            tginfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_tableoid));
            tginfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_oid));
            AssignDumpId(&tginfo[j].dobj);
            tginfo[j].dobj.name = pg_strdup(PQgetvalue(res, j, i_tgname));
            tginfo[j].dobj.namespace = tbinfo->dobj.namespace;
            tginfo[j].tgtable = tbinfo;
            tginfo[j].tgenabled = *(PQgetvalue(res, j, i_tgenabled));
            tginfo[j].tgispartition = *(PQgetvalue(res, j, i_tgispartition)) == 't';
            tginfo[j].tgdef = pg_strdup(PQgetvalue(res, j, i_tgdef));
        }
    }

    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(tbloids);
}
```