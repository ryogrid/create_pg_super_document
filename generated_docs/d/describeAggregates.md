# describeAggregates

## Location
[src/bin/psql/describe.c:71-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L71-L140)

## Overview
Implements the \da psql command to display a list of aggregate functions in the database, with optional pattern matching and filtering capabilities.

## Definition

```c
bool
describeAggregates(const char *pattern, bool verbose, bool showSystem)
```
## Detailed Description
This function generates and executes a SQL query to list aggregate functions from the PostgreSQL system catalogs. It constructs a SELECT query that retrieves aggregate function information from pg_proc and pg_namespace catalogs, formatting the output as a table showing schema name, function name, return type, and argument types. The function handles version-specific differences in PostgreSQL (using prokind='a' for version 11+ and proisagg for older versions) and supports pattern-based filtering and system object visibility control.

## Parameters / Member Variables
- `*pattern`: Optional regular expression pattern to filter aggregate functions by name or schema
- `verbose`: Flag to enable verbose output (currently not used in implementation)
- `showSystem`: Boolean flag to control whether system schema aggregates (pg_catalog, information_schema) are displayed
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c:836)

## Notes and Other Information
- Part of psql's describe functionality (\da command)
- Handles PostgreSQL version compatibility (version 11+ vs older versions)
- Uses internationalization through gettext_noop for column headers
- Returns boolean indicating success/failure of the operation
- Excludes system schemas by default unless showSystem is true
- The verbose parameter is accepted but not currently utilized in the implementation

## Simplified Source

```c
bool
describeAggregates(const char *pattern, bool verbose, bool showSystem)
{
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    initPQExpBuffer(&buf);

    // Build base query for aggregate functions
    printfPQExpBuffer(&buf,
                      "SELECT n.nspname as \"%s\",\n"
                      "  p.proname AS \"%s\",\n"
                      "  pg_catalog.format_type(p.prorettype, NULL) AS \"%s\",\n"
                      "  CASE WHEN p.pronargs = 0\n"
                      "    THEN CAST('*' AS pg_catalog.text)\n"
                      "    ELSE pg_catalog.pg_get_function_arguments(p.oid)\n"
                      "  END AS \"%s\",\n",
                      gettext_noop("Schema"),
                      gettext_noop("Name"),
                      gettext_noop("Result data type"),
                      gettext_noop("Argument data types"));

    // Add description and FROM clause (version-dependent)
    if (pset.sversion >= 110000)
        appendPQExpBuffer(&buf,
                          "  pg_catalog.obj_description(p.oid, 'pg_proc') as \"%s\"\n"
                          "FROM pg_catalog.pg_proc p\n"
                          "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n"
                          "WHERE p.prokind = 'a'\n",  // PostgreSQL 11+ aggregate identification
                          gettext_noop("Description"));
    else
        appendPQExpBuffer(&buf,
                          "  pg_catalog.obj_description(p.oid, 'pg_proc') as \"%s\"\n"
                          "FROM pg_catalog.pg_proc p\n"
                          "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n"
                          "WHERE p.proisagg\n",  // Pre-11 aggregate identification
                          gettext_noop("Description"));

    // Filter out system schemas if not requested
    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf, "      AND n.nspname <> 'pg_catalog'\n"
                                   "      AND n.nspname <> 'information_schema'\n");

    // Apply pattern filtering
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                                "n.nspname", "p.proname", NULL,
                                "pg_catalog.pg_function_is_visible(p.oid)",
                                NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1, 2, 4;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of aggregate functions");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);
    return true;
}
```