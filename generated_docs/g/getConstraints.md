# getConstraints

## Location
[src/bin/pg_dump/pg_dump.c:7822-7986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7822-L7986)

## Overview
Retrieves information about foreign key constraints on dumpable tables and creates corresponding ConstraintInfo entries for proper dependency handling during pg_dump operations.

## Definition

```c
void
getConstraints(Archive *fout, TableInfo tblinfo[], int numTables)
```
## Detailed Description
The getConstraints function focuses specifically on foreign key constraints, as other constraint types are handled elsewhere in the pg_dump process (unique/primary key constraints are managed with indexes, and check constraints are processed in getTableAttrs). The function constructs an optimized SQL query against pg_constraint using an OID array to limit results to tables of interest and having appropriate locks. It handles version-specific features like conindid column availability (PostgreSQL 11+) and conparentid filtering for inherited constraints. For each foreign key constraint found, it creates a ConstraintInfo structure with complete metadata and establishes proper dependencies. Special handling is implemented for foreign keys referencing partitioned tables, where the constraint must depend on partition index attach objects to ensure correct restoration order during database recovery.

## Parameters / Member Variables
- `*fout`: Archive pointer containing dump configuration and database connection information
- `tblinfo[]`: Array of TableInfo structures representing tables to be dumped
- `numTables`: Number of entries in the tblinfo array
## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md) (structure type)
  - [ConstraintInfo](../C/ConstraintInfo.md) (structure type)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - DUMP_COMPONENT_DEFINITION (flag constant)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK (constant)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - DO_FK_CONSTRAINT (enum value)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findTableByOid](../f/findTableByOid.md)
  - [IndxInfo](../I/IndxInfo.md) (structure type)
  - [addConstrChildIdxDeps](../a/addConstrChildIdxDeps.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)
  - [SubRelInfo](../S/SubRelInfo.md) (referenced in header)

## Notes and Other Information
- Only processes foreign key constraints (contype = 'f'); other constraint types are handled by different functions
- Includes tables with triggers or partitioned tables, as partitioned tables can have foreign keys without triggers
- Builds an OID array of target tables to create an efficient single-query approach for constraint retrieval
- Handles PostgreSQL version differences with conditional SQL for conindid and conparentid columns
- Creates dependency relationships for foreign keys pointing to partitioned tables to ensure proper index attachment ordering
- The function assumes tblinfo array is sorted by OID for efficient table lookup during constraint processing
- All created ConstraintInfo objects are marked as separate dump objects with proper namespace inheritance
- Memory allocation for ConstraintInfo array is based on the actual number of foreign key constraints found

## Simplified Source

```c
void getConstraints(Archive *fout, TableInfo tblinfo[], int numTables) {
    PQExpBuffer query = createPQExpBuffer();
    PQExpBuffer tbloids = createPQExpBuffer();

    // Build array of table OIDs to query constraints for
    appendPQExpBufferChar(tbloids, '{');
    for (int i = 0; i < numTables; i++) {
        TableInfo *tinfo = &tblinfo[i];

        // Skip tables without triggers (except partitioned tables)
        if ((!tinfo->hastriggers && tinfo->relkind != RELKIND_PARTITIONED_TABLE) ||
            !(tinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
            continue;

        if (tbloids->len > 1)
            appendPQExpBufferChar(tbloids, ',');
        appendPQExpBuffer(tbloids, "%u", tinfo->dobj.catId.oid);
    }
    appendPQExpBufferChar(tbloids, '}');

    // Build SQL query for foreign key constraints
    appendPQExpBufferStr(query,
        "SELECT c.tableoid, c.oid, conrelid, conname, confrelid, ");
    if (fout->remoteVersion >= 110000)
        appendPQExpBufferStr(query, "conindid, ");
    else
        appendPQExpBufferStr(query, "0 AS conindid, ");
    appendPQExpBuffer(query,
        "pg_catalog.pg_get_constraintdef(c.oid) AS condef\n"
        "FROM unnest('%s'::pg_catalog.oid[]) AS src(tbloid)\n"
        "JOIN pg_catalog.pg_constraint c ON (src.tbloid = c.conrelid)\n"
        "WHERE contype = 'f' ", tbloids->data);
    if (fout->remoteVersion >= 110000)
        appendPQExpBufferStr(query, "AND conparentid = 0 ");
    appendPQExpBufferStr(query, "ORDER BY conrelid, conname");

    // Execute query and process results
    PGresult *res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    int ntups = PQntuples(res);

    // Get column indices
    int i_contableoid = PQfnumber(res, "tableoid");
    int i_conoid = PQfnumber(res, "oid");
    int i_conrelid = PQfnumber(res, "conrelid");
    int i_conname = PQfnumber(res, "conname");
    int i_confrelid = PQfnumber(res, "confrelid");
    int i_conindid = PQfnumber(res, "conindid");
    int i_condef = PQfnumber(res, "condef");

    // Allocate constraint info array
    ConstraintInfo *constrinfo = pg_malloc(ntups * sizeof(ConstraintInfo));

    // Process each constraint result
    int curtblindx = -1;
    TableInfo *tbinfo = NULL;
    for (int j = 0; j < ntups; j++) {
        Oid conrelid = atooid(PQgetvalue(res, j, i_conrelid));

        // Find associated table (relies on OID-sorted tblinfo array)
        if (tbinfo == NULL || tbinfo->dobj.catId.oid != conrelid) {
            while (++curtblindx < numTables) {
                tbinfo = &tblinfo[curtblindx];
                if (tbinfo->dobj.catId.oid == conrelid)
                    break;
            }
        }

        // Initialize constraint info structure
        constrinfo[j].dobj.objType = DO_FK_CONSTRAINT;
        constrinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_contableoid));
        constrinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_conoid));
        AssignDumpId(&constrinfo[j].dobj);
        constrinfo[j].dobj.name = pg_strdup(PQgetvalue(res, j, i_conname));
        constrinfo[j].dobj.namespace = tbinfo->dobj.namespace;
        constrinfo[j].contable = tbinfo;
        constrinfo[j].contype = 'f';
        constrinfo[j].condef = pg_strdup(PQgetvalue(res, j, i_condef));
        constrinfo[j].confrelid = atooid(PQgetvalue(res, j, i_confrelid));
        constrinfo[j].separate = true;

        // Handle partitioned table dependencies
        TableInfo *reftable = findTableByOid(constrinfo[j].confrelid);
        if (reftable && reftable->relkind == RELKIND_PARTITIONED_TABLE) {
            Oid indexOid = atooid(PQgetvalue(res, j, i_conindid));
            if (indexOid != InvalidOid) {
                // Find and add dependencies on partition index attach objects
                for (int k = 0; k < reftable->numIndexes; k++) {
                    if (reftable->indexes[k].dobj.catId.oid == indexOid) {
                        addConstrChildIdxDeps(&constrinfo[j].dobj, &reftable->indexes[k]);
                        break;
                    }
                }
            }
        }
    }

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(tbloids);
}
```