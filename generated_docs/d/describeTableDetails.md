# describeTableDetails

## Location
[src/bin/psql/describe.c:1445-1527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L1445-L1527)

## Overview
Orchestrates the \d command in psql by finding matching tables and calling describeOneTableDetails for each one to display detailed information.

## Definition

```c
bool
describeTableDetails(const char *pattern, bool verbose, bool showSystem)
```
## Detailed Description
The  function serves as the main coordinator for the psql \d command. It performs a two-phase operation: first, it queries the PostgreSQL system catalogs to find all tables/relations that match the given pattern and filtering criteria, then it iterates through the results and calls  for each individual table to display its detailed information.

The function constructs a SQL query that retrieves the OID, schema name, and relation name from pg_class joined with pg_namespace. It applies filtering based on the showSystem flag and validates the provided pattern. For each matching relation found, it delegates the actual description display to .

This design allows for efficient batch processing when multiple tables match a pattern (e.g., \d public.*) while maintaining clean separation of concerns between table discovery and individual table description.

## Parameters / Member Variables
- `*pattern`: SQL pattern to filter table names (supports wildcards). If NULL, shows all visible tables.
- `verbose`: Boolean flag indicating verbose mode (\d+ vs \d). Passed through to describeOneTableDetails.
- `showSystem`: Boolean flag to include system catalogs (pg_catalog, information_schema). If false, only user tables are shown.
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md): Initialize query buffer for SQL construction
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md): Format SQL query into buffer
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md): Process and validate the name pattern for SQL
  - [PSQLexec](../P/PSQLexec.md): Execute the table discovery query
  - [describeOneTableDetails](describeOneTableDetails.md): Display detailed information for each individual table
  - [termPQExpBuffer](../t/termPQExpBuffer.md): Clean up query buffer
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md): Command dispatcher for \d commands in psql

## Notes and Other Information
- The function handles the case where no matching relations are found with appropriate error messages
- Uses ORDER BY to ensure consistent output ordering by schema and table name
- Includes cancel_pressed checking to allow user interruption during long operations
- Error handling includes proper cleanup of PostgreSQL result sets and buffers
- The function serves as a bridge between pattern matching and individual table description
- Returns false on any error (no matches, SQL errors, or cancellation) for proper error propagation

## Simplified Source

```c
bool describeTableDetails(const char *pattern, bool verbose, bool showSystem) {
    PQExpBufferData buf;
    PGresult *res;
    int i;

    initPQExpBuffer(&buf);

    // Query to find all matching tables/relations
    printfPQExpBuffer(&buf,
        "SELECT c.oid,\n"
        "  n.nspname,\n"
        "  c.relname\n"
        "FROM pg_catalog.pg_class c\n"
        "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n");

    // Filter out system schemas unless requested
    if (!showSystem && !pattern) {
        appendPQExpBufferStr(&buf,
            "WHERE n.nspname <> 'pg_catalog'\n"
            "      AND n.nspname <> 'information_schema'\n");
    }

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern, !showSystem && !pattern, false,
                                "n.nspname", "c.relname", NULL,
                                "pg_catalog.pg_table_is_visible(c.oid)",
                                NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    // Execute query to get matching tables
    appendPQExpBufferStr(&buf, "ORDER BY 2, 3;");
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    // Check if any tables were found
    if (PQntuples(res) == 0) {
        if (!pset.quiet) {
            if (pattern)
                pg_log_error("Did not find any relation named \"%s\".", pattern);
            else
                pg_log_error("Did not find any relations.");
        }
        PQclear(res);
        return false;
    }

    // Process each matching table
    for (i = 0; i < PQntuples(res); i++) {
        const char *oid = PQgetvalue(res, i, 0);
        const char *nspname = PQgetvalue(res, i, 1);
        const char *relname = PQgetvalue(res, i, 2);

        // Display details for this table
        if (!describeOneTableDetails(nspname, relname, oid, verbose)) {
            PQclear(res);
            return false;
        }

        // Check for user cancellation
        if (cancel_pressed) {
            PQclear(res);
            return false;
        }
    }

    PQclear(res);
    return true;
}
```