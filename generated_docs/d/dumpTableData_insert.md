# dumpTableData_insert

## Location
[src/bin/pg_dump/pg_dump.c:2334-2602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2334-L2602)

## Overview
Dumps table data using INSERT statements, providing an alternative to COPY that is more portable and suitable for cross-database exports and smaller data sets.

## Definition

```c
static int
dumpTableData_insert(Archive *fout, const void *dcontext)
```
## Detailed Description
This function generates INSERT statements to dump table data, offering more portability than COPY commands. It handles various PostgreSQL data types with appropriate formatting, supports generated columns, and can produce either column-named or positional INSERTs. The function uses a cursor-based approach to fetch data in chunks and supports multi-row INSERT statements for efficiency.

Key features include special handling for generated columns (either excluded or replaced with DEFAULT), proper formatting for different data types (numeric, boolean, bit strings, etc.), support for partition tables with optional root table loading, and conflict resolution with ON CONFLICT DO NOTHING option.

## Parameters / Member Variables
- `*fout`: Pointer to the Archive structure containing dump configuration and output context
- `*dcontext`: Void pointer that contains TableDataInfo structure cast as context data
## Dependencies
- Functions called/Symbols referenced:
  - [TableDataInfo](../T/TableDataInfo.md) (struct)
  - [TableInfo](../T/TableInfo.md) (struct)
  - DumpOptions (struct)
  - RELKIND_FOREIGN_TABLE
  - [set_restrict_relation_kind](../s/set_restrict_relation_kind.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK
  - [PQnfields](../P/PQnfields.md)
  - [forcePartitionRootLoad](../f/forcePartitionRootLoad.md)
  - [getRootTableInfo](../g/getRootTableInfo.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [PQfname](../P/PQfname.md)
  - [archputs](../a/archputs.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQftype](../P/PQftype.md)
  - [archprintf](../a/archprintf.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - appendStringLiteralAH
- Called from (representative examples):
  - [dumpTableData](dumpTableData.md)

## Notes and Other Information
- Generates INSERT statements that are compatible with pg_backup_db.c's ExecuteSimpleCommands()
- Avoids comments, E'' strings, and dollar-quoted strings for compatibility
- Handles generated columns by excluding them from column lists or using DEFAULT values
- Uses cursor-based fetching (FETCH 100) to process large tables efficiently
- Supports --rows-per-insert option for multi-row INSERT statements
- Provides special formatting for numeric types, bit strings, and boolean values
- Can target partition root tables when load-via-partition-root is enabled
- Includes ON CONFLICT DO NOTHING option for conflict resolution
- Handles zero-column tables with DEFAULT VALUES syntax
- Returns 1 on success, with comprehensive error handling throughout

## Simplified Source

```c
static int
dumpTableData_insert(Archive *fout, const void *dcontext)
{
    TableDataInfo *tdinfo = (TableDataInfo *) dcontext;
    TableInfo  *tbinfo = tdinfo->tdtable;
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query = createPQExpBuffer();
    PQExpBuffer insertStmt = NULL;
    char       *attgenerated;
    PGresult   *res;
    int         nfields, rows_per_statement = dopt->dump_inserts;
    int         rows_this_statement = 0;

    // Handle foreign tables
    if (tbinfo->relkind == RELKIND_FOREIGN_TABLE)
        set_restrict_relation_kind(fout, "view");

    // Build SELECT query, handling generated columns
    attgenerated = (char *) pg_malloc(tbinfo->numatts * sizeof(char));
    appendPQExpBufferStr(query, "DECLARE _pg_dump_cursor CURSOR FOR SELECT ");
    nfields = 0;

    for (int i = 0; i < tbinfo->numatts; i++) {
        if (tbinfo->attisdropped[i])
            continue;
        if (tbinfo->attgenerated[i] && dopt->column_inserts)
            continue;  // Skip generated columns for column inserts

        if (nfields > 0)
            appendPQExpBufferStr(query, ", ");

        if (tbinfo->attgenerated[i])
            appendPQExpBufferStr(query, "NULL");  // Placeholder for generated columns
        else
            appendPQExpBufferStr(query, fmtId(tbinfo->attnames[i]));

        attgenerated[nfields] = tbinfo->attgenerated[i];
        nfields++;
    }

    if (nfields == 0)
        appendPQExpBufferStr(query, "NULL");  // Handle zero-column tables

    appendPQExpBuffer(query, " FROM ONLY %s", fmtQualifiedDumpable(tbinfo));
    if (tdinfo->filtercond)
        appendPQExpBuffer(query, " %s", tdinfo->filtercond);

    ExecuteSqlStatement(fout, query->data);

    // Process data in chunks
    while (1) {
        res = ExecuteSqlQuery(fout, "FETCH 100 FROM _pg_dump_cursor", PGRES_TUPLES_OK);

        // Build INSERT statement template on first iteration
        if (insertStmt == NULL) {
            TableInfo *targettab;
            insertStmt = createPQExpBuffer();

            // Determine target table (might be partition root)
            if (tbinfo->ispartition &&
                (dopt->load_via_partition_root || forcePartitionRootLoad(tbinfo)))
                targettab = getRootTableInfo(tbinfo);
            else
                targettab = tbinfo;

            appendPQExpBuffer(insertStmt, "INSERT INTO %s ", fmtQualifiedDumpable(targettab));

            if (nfields == 0) {
                appendPQExpBufferStr(insertStmt, "DEFAULT VALUES;\n");
            } else {
                // Add column names if requested
                if (dopt->column_inserts) {
                    appendPQExpBufferChar(insertStmt, '(');
                    for (int field = 0; field < nfields; field++) {
                        if (field > 0)
                            appendPQExpBufferStr(insertStmt, ", ");
                        appendPQExpBufferStr(insertStmt, fmtId(PQfname(res, field)));
                    }
                    appendPQExpBufferStr(insertStmt, ") ");
                }

                if (tbinfo->needs_override)
                    appendPQExpBufferStr(insertStmt, "OVERRIDING SYSTEM VALUE ");

                appendPQExpBufferStr(insertStmt, "VALUES");
            }
        }

        // Process each tuple in the result set
        for (int tuple = 0; tuple < PQntuples(res); tuple++) {
            if (rows_this_statement == 0)
                archputs(insertStmt->data, fout);

            if (nfields == 0)
                continue;  // Zero-column table - statement already complete

            // Format row data
            if (rows_per_statement == 1)
                archputs(" (", fout);
            else if (rows_this_statement > 0)
                archputs(",\n\t(", fout);
            else
                archputs("\n\t(", fout);

            // Output each field value
            for (int field = 0; field < nfields; field++) {
                if (field > 0)
                    archputs(", ", fout);

                if (attgenerated[field]) {
                    archputs("DEFAULT", fout);
                    continue;
                }

                if (PQgetisnull(res, tuple, field)) {
                    archputs("NULL", fout);
                    continue;
                }

                // Format value based on data type
                const char *value = PQgetvalue(res, tuple, field);
                switch (PQftype(res, field)) {
                    case INT2OID: case INT4OID: case INT8OID:
                    case OIDOID: case FLOAT4OID: case FLOAT8OID: case NUMERICOID:
                        // Numeric types - quote only special values
                        if (strspn(value, "0123456789 +-eE.") == strlen(value))
                            archputs(value, fout);
                        else
                            archprintf(fout, "'%s'", value);
                        break;
                    case BITOID: case VARBITOID:
                        archprintf(fout, "B'%s'", value);
                        break;
                    case BOOLOID:
                        archputs(strcmp(value, "t") == 0 ? "true" : "false", fout);
                        break;
                    default:
                        // String literal for all other types
                        resetPQExpBuffer(query);
                        appendStringLiteralAH(query, value, fout);
                        archputs(query->data, fout);
                        break;
                }
            }

            archputs(")", fout);

            // Complete statement if row limit reached
            if (++rows_this_statement >= rows_per_statement) {
                if (dopt->do_nothing)
                    archputs(" ON CONFLICT DO NOTHING;\n", fout);
                else
                    archputs(";\n", fout);
                rows_this_statement = 0;
            }
        }

        if (PQntuples(res) <= 0) {
            PQclear(res);
            break;
        }
        PQclear(res);
    }

    // Complete any partial statement
    if (rows_this_statement > 0) {
        if (dopt->do_nothing)
            archputs(" ON CONFLICT DO NOTHING;\n", fout);
        else
            archputs(";\n", fout);
    }

    archputs("\n\n", fout);

    // Cleanup
    ExecuteSqlStatement(fout, "CLOSE _pg_dump_cursor");
    destroyPQExpBuffer(query);
    if (insertStmt != NULL)
        destroyPQExpBuffer(insertStmt);
    free(attgenerated);

    if (tbinfo->relkind == RELKIND_FOREIGN_TABLE)
        set_restrict_relation_kind(fout, "view, foreign-table");

    return 1;
}
```