# objectDescription

## Location
[src/bin/psql/describe.c:1252-1444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L1252-L1444)

## Overview
Implements the \dd command in psql to display comments for database objects that don't have their own dedicated describe commands.

## Definition

```c
bool
objectDescription(const char *pattern, bool showSystem)
```
## Detailed Description
The  function implements the psql \dd command, which lists comments for specific types of database objects. Unlike other describe commands that show comprehensive object information, this function focuses solely on retrieving and displaying user-defined comments/descriptions for objects.

The function specifically handles these object types:
- Table constraints (check, foreign key, unique, etc.)
- Domain constraints  
- Operator classes
- Operator families
- Rules (excluding view rules)
- Triggers

It constructs a complex SQL query that unions together queries for each object type, retrieving the schema name, object name, object type, and description from the PostgreSQL system catalogs. The results are formatted and displayed in a tabular format.

## Parameters / Member Variables
- `*pattern`: SQL pattern to filter object names (supports wildcards like *, ?, etc.). If NULL, shows all objects.
- `showSystem`: Boolean flag to include system objects (pg_catalog, information_schema). If false, only user objects are shown.
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md): Initialize query buffer
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md): Validate and process name patterns for SQL queries
  - [PSQLexec](../P/PSQLexec.md): Execute the constructed SQL query
  - [printQuery](../p/printQuery.md): Format and display query results
  - [termPQExpBuffer](../t/termPQExpBuffer.md): Clean up query buffer
  - lengthof: Get array length
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md): Main command dispatcher for \dd command in psql

## Notes and Other Information
- The function uses a UNION ALL approach to combine results from multiple system catalog queries
- System objects are filtered out by default unless showSystem is true
- Only objects with actual comments (entries in pg_description) are displayed
- The function handles internationalization through gettext_noop for column headers
- Error handling includes proper cleanup of allocated buffers on failure
- Results are ordered by schema, name, and object type for consistent display

## Simplified Source

```c
bool objectDescription(const char *pattern, bool showSystem) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    static const bool translate_columns[] = {false, false, true, false};

    initPQExpBuffer(&buf);

    // Build UNION query for multiple object types with comments
    appendPQExpBuffer(&buf,
        "SELECT DISTINCT tt.nspname AS \"Schema\", tt.name AS \"Name\", tt.object AS \"Object\", d.description AS \"Description\"\n"
        "FROM (\n");

    // Table constraints
    appendPQExpBuffer(&buf,
        "  SELECT pgc.oid as oid, pgc.tableoid AS tableoid,\n"
        "  n.nspname as nspname,\n"
        "  CAST(pgc.conname AS pg_catalog.text) as name,"
        "  CAST('table constraint' AS pg_catalog.text) as object\n"
        "  FROM pg_catalog.pg_constraint pgc\n"
        "    JOIN pg_catalog.pg_class c ON c.oid = pgc.conrelid\n"
        "    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n");

    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf, "WHERE n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'\n");

    if (!validateSQLNamePattern(&buf, pattern, !showSystem && !pattern,
                                false, "n.nspname", "pgc.conname", NULL,
                                "pg_catalog.pg_table_is_visible(c.oid)", NULL, 3))
        goto error_return;

    // Domain constraints
    appendPQExpBuffer(&buf,
        "UNION ALL\n"
        "  SELECT pgc.oid as oid, pgc.tableoid AS tableoid,\n"
        "  n.nspname as nspname,\n"
        "  CAST(pgc.conname AS pg_catalog.text) as name,"
        "  CAST('domain constraint' AS pg_catalog.text) as object\n"
        "  FROM pg_catalog.pg_constraint pgc\n"
        "    JOIN pg_catalog.pg_type t ON t.oid = pgc.contypid\n"
        "    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n");

    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf, "WHERE n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'\n");

    if (!validateSQLNamePattern(&buf, pattern, !showSystem && !pattern,
                                false, "n.nspname", "pgc.conname", NULL,
                                "pg_catalog.pg_type_is_visible(t.oid)", NULL, 3))
        goto error_return;

    // Operator classes
    appendPQExpBuffer(&buf,
        "UNION ALL\n"
        "  SELECT o.oid as oid, o.tableoid as tableoid,\n"
        "  n.nspname as nspname,\n"
        "  CAST(o.opcname AS pg_catalog.text) as name,\n"
        "  CAST('operator class' AS pg_catalog.text) as object\n"
        "  FROM pg_catalog.pg_opclass o\n"
        "    JOIN pg_catalog.pg_namespace n ON n.oid = o.opcnamespace\n");

    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf, "      AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'\n");

    if (!validateSQLNamePattern(&buf, pattern, true, false,
                                "n.nspname", "o.opcname", NULL,
                                "pg_catalog.pg_opclass_is_visible(o.oid)", NULL, 3))
        goto error_return;

    // Operator families
    appendPQExpBuffer(&buf,
        "UNION ALL\n"
        "  SELECT opf.oid as oid, opf.tableoid as tableoid,\n"
        "  n.nspname as nspname,\n"
        "  CAST(opf.opfname AS pg_catalog.text) AS name,\n"
        "  CAST('operator family' AS pg_catalog.text) as object\n"
        "  FROM pg_catalog.pg_opfamily opf\n"
        "    JOIN pg_catalog.pg_namespace n ON opf.opfnamespace = n.oid\n");

    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf, "      AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'\n");

    if (!validateSQLNamePattern(&buf, pattern, true, false,
                                "n.nspname", "opf.opfname", NULL,
                                "pg_catalog.pg_opfamily_is_visible(opf.oid)", NULL, 3))
        goto error_return;

    // Rules (excluding view rules)
    appendPQExpBuffer(&buf,
        "UNION ALL\n"
        "  SELECT r.oid as oid, r.tableoid as tableoid,\n"
        "  n.nspname as nspname,\n"
        "  CAST(r.rulename AS pg_catalog.text) as name,"
        "  CAST('rule' AS pg_catalog.text) as object\n"
        "  FROM pg_catalog.pg_rewrite r\n"
        "       JOIN pg_catalog.pg_class c ON c.oid = r.ev_class\n"
        "       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
        "  WHERE r.rulename != '_RETURN'\n");

    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf, "      AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'\n");

    if (!validateSQLNamePattern(&buf, pattern, true, false,
                                "n.nspname", "r.rulename", NULL,
                                "pg_catalog.pg_table_is_visible(c.oid)", NULL, 3))
        goto error_return;

    // Triggers
    appendPQExpBuffer(&buf,
        "UNION ALL\n"
        "  SELECT t.oid as oid, t.tableoid as tableoid,\n"
        "  n.nspname as nspname,\n"
        "  CAST(t.tgname AS pg_catalog.text) as name,"
        "  CAST('trigger' AS pg_catalog.text) as object\n"
        "  FROM pg_catalog.pg_trigger t\n"
        "       JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid\n"
        "       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n");

    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf, "WHERE n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'\n");

    if (!validateSQLNamePattern(&buf, pattern, !showSystem && !pattern, false,
                                "n.nspname", "t.tgname", NULL,
                                "pg_catalog.pg_table_is_visible(c.oid)", NULL, 3))
        goto error_return;

    // Join with descriptions and finalize query
    appendPQExpBufferStr(&buf,
        ") AS tt\n"
        "  JOIN pg_catalog.pg_description d ON (tt.oid = d.objoid AND tt.tableoid = d.classoid AND d.objsubid = 0)\n");

    appendPQExpBufferStr(&buf, "ORDER BY 1, 2, 3;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = "Object descriptions";
    myopt.translate_header = true;
    myopt.translate_columns = translate_columns;
    myopt.n_translate_columns = lengthof(translate_columns);

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```