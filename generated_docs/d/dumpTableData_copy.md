# dumpTableData_copy

## Location
[src/bin/pg_dump/pg_dump.c:2166-2333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2166-L2333)

## Overview
Dumps a table's contents using the PostgreSQL COPY command, which is the efficient method for extracting table data during database dumps.

## Definition

```c
structure tvi.
		 * Finally, call gettimeofday again to save the 'last sleep time'.
		 * ----------
		 */
	}
	archprintf(fout, "\\.\n\n\n");
```
## Detailed Description
This function implements the COPY-based table data dumping mechanism in pg_dump. It constructs and executes a COPY command to extract table data, handling various scenarios including foreign tables and filtered queries. The function uses explicit column ordering to ensure data is retrieved in the correct sequence, avoiding issues with column inheritance and ADD COLUMN operations.

For foreign tables and filtered queries, it uses COPY (SELECT ...) TO syntax, while for regular tables it uses the simpler COPY tablename TO syntax. The function includes comprehensive error handling and manages the COPY protocol communication with the PostgreSQL server.

## Parameters / Member Variables
- : Pointer to the Archive structure containing dump configuration and output context
- : Void pointer that contains TableDataInfo structure cast as context data

## Dependencies
- Functions called/Symbols referenced:
  - [TableDataInfo](../T/TableDataInfo.md) (struct)
  - [TableInfo](../T/TableInfo.md) (struct)  
  - [GetConnection](../G/GetConnection.md)
  - pg_log_info
  - [fmtCopyColumnList](../f/fmtCopyColumnList.md)
  - RELKIND_FOREIGN_TABLE
  - [set_restrict_relation_kind](../s/set_restrict_relation_kind.md)
  - fmtQualifiedDumpable
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_COPY_OUT
  - [PQgetCopyData](../P/PQgetCopyData.md)
  - [WriteData](../W/WriteData.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [archprintf](../a/archprintf.md)
  - pg_log_error_detail
  - [exit_nicely](../e/exit_nicely.md)
  - [PQgetResult](../P/PQgetResult.md)
  - PGRES_COMMAND_OK
  - pg_log_warning
- Called from (representative examples):
  - [dumpTableData](dumpTableData.md)

## Notes and Other Information
- Uses explicit column listing to avoid issues with column ordering in inheritance scenarios
- Handles foreign tables by temporarily adjusting relation kind restrictions
- Implements COPY protocol communication with proper error handling
- Contains extensive historical commentary about throttling mechanisms that were considered but not implemented
- Returns 1 on success, exits with error on failure
- Manages libpq connection state carefully to ensure proper cleanup
- Special handling for filtered queries and foreign tables using COPY (SELECT ...) syntax
- Outputs COPY termination sequence (\.\n\n\n) to mark end of data

## Simplified Source

```c
static int
dumpTableData_copy(Archive *fout, const void *dcontext)
{
    TableDataInfo *tdinfo = (TableDataInfo *) dcontext;
    TableInfo  *tbinfo = tdinfo->tdtable;
    const char *classname = tbinfo->dobj.name;
    PQExpBuffer query = createPQExpBuffer();
    PQExpBuffer columnList = createPQExpBuffer();
    PGconn     *conn = GetConnection(fout);
    PGresult   *res;
    int         ret;
    char       *copybuf;
    const char *column_list;

    pg_log_info("dumping contents of table \"%s.%s\"",
                tbinfo->dobj.namespace->dobj.name, classname);

    // Get explicit column list to ensure correct ordering
    column_list = fmtCopyColumnList(tbinfo, columnList);

    // Handle foreign tables and filtered queries with COPY (SELECT ...)
    if (tdinfo->filtercond || tbinfo->relkind == RELKIND_FOREIGN_TABLE) {
        if (tbinfo->relkind == RELKIND_FOREIGN_TABLE)
            set_restrict_relation_kind(fout, "view");

        appendPQExpBufferStr(query, "COPY (SELECT ");
        if (strlen(column_list) > 2) {
            appendPQExpBufferStr(query, column_list + 1);
            query->data[query->len - 1] = ' ';
        } else {
            appendPQExpBufferStr(query, "* ");
        }
        appendPQExpBuffer(query, "FROM %s %s) TO stdout;",
                         fmtQualifiedDumpable(tbinfo),
                         tdinfo->filtercond ? tdinfo->filtercond : "");
    } else {
        // Simple COPY for regular tables
        appendPQExpBuffer(query, "COPY %s %s TO stdout;",
                         fmtQualifiedDumpable(tbinfo), column_list);
    }

    // Execute COPY command and read data
    res = ExecuteSqlQuery(fout, query->data, PGRES_COPY_OUT);
    PQclear(res);
    destroyPQExpBuffer(columnList);

    // Read COPY data and write to output
    for (;;) {
        ret = PQgetCopyData(conn, &copybuf, 0);
        if (ret < 0)
            break;  // End of data or error

        if (copybuf) {
            WriteData(fout, copybuf, ret);
            PQfreemem(copybuf);
        }
    }

    archprintf(fout, "\\.\n\n\n");

    // Error handling for copy data transfer
    if (ret == -2) {
        pg_log_error("Dumping the contents of table \"%s\" failed: PQgetCopyData() failed.", classname);
        exit_nicely(1);
    }

    // Verify final command status
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        pg_log_error("Dumping the contents of table \"%s\" failed: PQgetResult() failed.", classname);
        exit_nicely(1);
    }
    PQclear(res);

    // Clean up
    destroyPQExpBuffer(query);
    if (tbinfo->relkind == RELKIND_FOREIGN_TABLE)
        set_restrict_relation_kind(fout, "view, foreign-table");

    return 1;
}
```