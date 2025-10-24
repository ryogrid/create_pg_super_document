# getTables

## Location
[src/bin/pg_dump/pg_dump.c:6806-7251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6806-L7251)

## Overview
The getTables function retrieves all tables and table-like objects from the PostgreSQL system catalogs and returns them in a TableInfo structure array for use by pg_dump, implementing sophisticated filtering and locking mechanisms.

## Definition

```c
TableInfo *
getTables(Archive *fout, int *numTables)
```
## Detailed Description
This function is one of the most comprehensive catalog reading functions in pg_dump. It constructs and executes a complex version-dependent SQL query to retrieve all table-like objects including regular tables, sequences, views, materialized views, foreign tables, partitioned tables, and composite types from pg_class and related system catalogs.

The function performs several critical operations: it collects comprehensive metadata for each table including relkind, namespace, owner, constraints, indexes, rules, pages, tablespaces, replication identity, row security, frozen transaction IDs, and access control information. It handles version-specific features like access methods (PostgreSQL 9.6+), identity sequences, partitioning (PostgreSQL 10+), and the removal of WITH OIDS (PostgreSQL 12+).

A key feature is its table locking mechanism that acquires ACCESS SHARE locks on dumpable tables in batches to prevent concurrent schema modifications during the dump process. The function also implements sophisticated dependency tracking for sequences and their owning tables, and handles toast table relationships while avoiding issues with partitioned table toast OIDs in certain PostgreSQL versions.

## Parameters / Member Variables
- `*fout`: Archive structure containing connection information and dump configuration options
- `*numTables`: Pointer to integer that will be set to the number of tables found
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [pg_malloc0](../p/pg_malloc0.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md)
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - atoi
  - strcmp
  - [PQgetisnull](../P/PQgetisnull.md)
  - [selectDumpableTable](../s/selectDumpableTable.md)
  - fmtQualifiedDumpable
  - [GetConnection](../G/GetConnection.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Retrieves all relkinds including relations, sequences, views, composite types, materialized views, foreign tables, and partitioned tables
- Implements version-specific SQL queries to handle PostgreSQL evolution (access methods, identity sequences, partitioning, WITH OIDS removal)
- Uses batch locking mechanism to acquire ACCESS SHARE locks on dumpable tables to prevent schema changes during dump
- Handles complex join relationships with pg_depend for sequence ownership tracking and pg_tablespace for tablespace information
- Manages toast table relationships while avoiding version-specific issues with partitioned table toast OIDs
- Implements sophisticated filtering logic to determine which tables are "interesting" for dump purposes
- Supports lock timeout configuration to avoid indefinite waiting for table locks
- Uses DO_TABLE object type identifier for dump object classification
- Preserves comprehensive metadata including frozen XIDs, multixact IDs, replication identity, row security settings
- Handles foreign table server dependencies and access method information for modern PostgreSQL versions

## Simplified Source

```c
TableInfo *
getTables(Archive *fout, int *numTables)
{
    DumpOptions *dopt = fout->dopt;
    PGresult *res;
    int ntups, i;
    PQExpBuffer query = createPQExpBuffer();
    TableInfo *tblinfo;
    // Column index variables
    int i_reltableoid, i_reloid, i_relname, i_relnamespace, i_relkind;
    int i_reltype, i_relowner, i_relacl, i_acldefault;
    // ... many other column indices for table metadata

    // Build complex version-dependent query for all table-like objects
    appendPQExpBufferStr(query,
                         "SELECT c.tableoid, c.oid, c.relname, "
                         "c.relnamespace, c.relkind, c.reltype, c.relowner, "
                         "c.relchecks, c.relhasindex, c.relhasrules, c.relpages, "
                         "c.relhastriggers, c.relpersistence, c.reloftype, c.relacl, "
                         "acldefault(CASE WHEN c.relkind = 'S' THEN 's'::\"char\" "
                         "ELSE 'r'::\"char\" END, c.relowner) AS acldefault, "
                         "CASE WHEN c.relkind = 'f' THEN "
                         "(SELECT ftserver FROM pg_catalog.pg_foreign_table WHERE ftrelid = c.oid) "
                         "ELSE 0 END AS foreignserver, "
                         "c.relfrozenxid, tc.relfrozenxid AS tfrozenxid, "
                         "tc.oid AS toid, tc.relpages AS toastpages, "
                         "tc.reloptions AS toast_reloptions, "
                         "d.refobjid AS owning_tab, d.refobjsubid AS owning_col, "
                         "tsp.spcname AS reltablespace, ");

    // Add version-specific columns
    if (fout->remoteVersion >= 120000)
        appendPQExpBufferStr(query, "false AS relhasoids, ");
    else
        appendPQExpBufferStr(query, "c.relhasoids, ");

    if (fout->remoteVersion >= 90300)
        appendPQExpBufferStr(query, "c.relispopulated, ");
    else
        appendPQExpBufferStr(query, "'t' as relispopulated, ");

    // Add other version-specific fields (replication identity, row security, etc.)

    // FROM clause with complex joins
    appendPQExpBufferStr(query,
                         "FROM pg_class c "
                         "LEFT JOIN pg_depend d ON (c.relkind = 'S' AND "
                         "d.classid = 'pg_class'::regclass AND d.objid = c.oid AND "
                         "d.objsubid = 0 AND d.refclassid = 'pg_class'::regclass "
                         "AND d.deptype IN ('a', 'i')) "
                         "LEFT JOIN pg_tablespace tsp ON (tsp.oid = c.reltablespace) ");

    if (fout->remoteVersion >= 90600)
        appendPQExpBufferStr(query, "LEFT JOIN pg_am am ON (c.relam = am.oid) ");

    appendPQExpBufferStr(query,
                         "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid "
                         "AND tc.relkind = 't' AND c.relkind <> 'p') "
                         "WHERE c.relkind IN ('r', 'S', 'v', 'c', 'm', 'f', 'p') "
                         "ORDER BY c.oid");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numTables = ntups;

    // Allocate and populate table info array
    tblinfo = (TableInfo *) pg_malloc0(ntups * sizeof(TableInfo));

    // Get all column indices
    i_reltableoid = PQfnumber(res, "tableoid");
    i_reloid = PQfnumber(res, "oid");
    i_relname = PQfnumber(res, "relname");
    // ... get other column indices

    // Set lock timeout if specified
    if (dopt->lockWaitTimeout) {
        resetPQExpBuffer(query);
        appendPQExpBuffer(query, "SET statement_timeout = %s", dopt->lockWaitTimeout);
        ExecuteSqlStatement(fout, query->data);
    }

    resetPQExpBuffer(query);

    // Process each table and acquire locks in batches
    for (i = 0; i < ntups; i++) {
        tblinfo[i].dobj.objType = DO_TABLE;
        tblinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_reltableoid));
        tblinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_reloid));
        AssignDumpId(&tblinfo[i].dobj);
        tblinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_relname));
        tblinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_relnamespace)));

        // Set table properties
        tblinfo[i].relkind = *(PQgetvalue(res, i, i_relkind));
        tblinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_relowner));
        tblinfo[i].hasindex = (strcmp(PQgetvalue(res, i, i_relhasindex), "t") == 0);
        tblinfo[i].hasrules = (strcmp(PQgetvalue(res, i, i_relhasrules), "t") == 0);
        // ... set other properties

        // Determine if table should be dumped
        if (tblinfo[i].relkind == RELKIND_COMPOSITE_TYPE)
            tblinfo[i].dobj.dump = DUMP_COMPONENT_NONE;
        else
            selectDumpableTable(&tblinfo[i], fout);

        tblinfo[i].interesting = (tblinfo[i].dobj.dump &
                                  (DUMP_COMPONENT_DEFINITION | DUMP_COMPONENT_DATA)) != 0;

        // Batch table locking for dumpable tables
        if ((tblinfo[i].dobj.dump & DUMP_COMPONENTS_REQUIRING_LOCK) &&
            (tblinfo[i].relkind == RELKIND_RELATION ||
             tblinfo[i].relkind == RELKIND_PARTITIONED_TABLE)) {

            if (query->len == 0)
                appendPQExpBuffer(query, "LOCK TABLE %s", fmtQualifiedDumpable(&tblinfo[i]));
            else {
                appendPQExpBuffer(query, ", %s", fmtQualifiedDumpable(&tblinfo[i]));

                // Execute batch when query gets large
                if (query->len >= 100000) {
                    appendPQExpBufferStr(query, " IN ACCESS SHARE MODE");
                    ExecuteSqlStatement(fout, query->data);
                    resetPQExpBuffer(query);
                }
            }
        }
    }

    // Lock remaining tables in final batch
    if (query->len != 0) {
        appendPQExpBufferStr(query, " IN ACCESS SHARE MODE");
        ExecuteSqlStatement(fout, query->data);
    }

    // Reset lock timeout
    if (dopt->lockWaitTimeout)
        ExecuteSqlStatement(fout, "SET statement_timeout = 0");

    PQclear(res);
    destroyPQExpBuffer(query);
    return tblinfo;
}
```