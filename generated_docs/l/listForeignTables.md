# listForeignTables

## Location
[src/bin/psql/describe.c:5930-6001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5930-L6001)

## Overview
Implements the  command in psql to list foreign tables with their associated schemas, servers, and optional FDW options and descriptions.

## Definition

```c
bool
listForeignTables(const char *pattern, bool verbose)
```
## Detailed Description
This function queries the PostgreSQL system catalogs to retrieve information about foreign tables. It constructs a SQL query that joins multiple system tables (pg_foreign_table, pg_class, pg_namespace, pg_foreign_server, and optionally pg_description) to present a comprehensive view of foreign tables. The function supports pattern matching for selective display and verbose mode for additional details like FDW options and descriptions.

The query retrieves:
- Schema name (namespace)
- Table name
- Server name
- FDW options (in verbose mode)
- Table description (in verbose mode)

## Parameters / Member Variables
- `*pattern`: SQL name pattern for filtering foreign tables (can be NULL for all tables)
- `verbose`: Boolean flag to include additional information like FDW options and descriptions
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (data structure)
  - [printQueryOpt](../p/printQueryOpt.md) (data structure)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in src/bin/psql/command.c:1003)

## Notes and Other Information
- This function is part of psql's describe commands (\d family)
- Uses internationalization with gettext_noop for column headers
- Implements proper error handling by returning false on failures
- The query joins multiple system catalogs to provide comprehensive foreign table information
- Pattern validation is handled by validateSQLNamePattern to ensure SQL injection safety
- Results are formatted and displayed using psql's standard query printing mechanism

## Simplified Source

```c
bool listForeignTables(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Initialize buffer and build base query
    initPQExpBuffer(&buf);
    printfPQExpBuffer(&buf,
        "SELECT n.nspname AS \"%s\",\n"
        "  c.relname AS \"%s\",\n"
        "  s.srvname AS \"%s\"",
        gettext_noop("Schema"),
        gettext_noop("Table"),
        gettext_noop("Server"));

    // Add verbose columns if requested
    if (verbose) {
        appendPQExpBuffer(&buf,
            ",\n CASE WHEN ftoptions IS NULL THEN '' ELSE "
            "  '(' || pg_catalog.array_to_string(ARRAY(SELECT "
            "  pg_catalog.quote_ident(option_name) ||  ' ' || "
            "  pg_catalog.quote_literal(option_value)  FROM "
            "  pg_catalog.pg_options_to_table(ftoptions)),  ', ') || ')' "
            "  END AS \"%s\",\n"
            "  d.description AS \"%s\"",
            gettext_noop("FDW options"),
            gettext_noop("Description"));
    }

    // Add FROM clause with JOINs
    appendPQExpBufferStr(&buf,
        "\nFROM pg_catalog.pg_foreign_table ft\n"
        "  INNER JOIN pg_catalog.pg_class c"
        " ON c.oid = ft.ftrelid\n"
        "  INNER JOIN pg_catalog.pg_namespace n"
        " ON n.oid = c.relnamespace\n"
        "  INNER JOIN pg_catalog.pg_foreign_server s"
        " ON s.oid = ft.ftserver\n");

    // Add description join for verbose mode
    if (verbose) {
        appendPQExpBufferStr(&buf,
            "   LEFT JOIN pg_catalog.pg_description d\n"
            "          ON d.classoid = c.tableoid AND "
            "d.objoid = c.oid AND d.objsubid = 0\n");
    }

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                "n.nspname", "c.relname", NULL,
                                "pg_catalog.pg_table_is_visible(c.oid)",
                                NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1, 2;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of foreign tables");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    return true;
}
```