# listDbRoleSettings

## Location
[src/bin/psql/describe.c:3761-3829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L3761-L3829)

## Overview
A psql command function that implements the \\drds (describe role database settings) metacommand to display role-specific database configuration settings.

## Definition

```c
bool
listDbRoleSettings(const char *pattern, const char *pattern2)
```
## Detailed Description
This function provides functionality for the psql \\drds metacommand, which displays configuration settings that are specific to combinations of database roles and databases. It queries the pg_db_role_setting system catalog to retrieve role-specific and database-specific parameter settings. The function supports pattern matching for both role names and database names, allowing users to filter results. It constructs and executes a SQL query that joins pg_db_role_setting with pg_database and pg_roles catalogs to provide human-readable output with role names, database names, and their associated configuration settings.

## Parameters / Member Variables
- `*pattern`: A SQL pattern (with wildcards) to filter by role name, or NULL to match all roles
- `*pattern2`: A SQL pattern (with wildcards) to filter by database name, or NULL to match all databases
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (PostgreSQL's expandable string buffer structure)
  - [printQueryOpt](../p/printQueryOpt.md) (print formatting options structure)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formatted append to buffer)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and append SQL name patterns)
  - [PSQLexec](../P/PSQLexec.md) (execute SQL query)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup buffer)
  - [printQuery](../p/printQuery.md) (display query results)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher at src/bin/psql/command.c:941)
  - DESCRIBE_H (function declaration in src/bin/psql/describe.h:38)

## Notes and Other Information
- Returns true on success, false on error
- Implements the psql \\drds metacommand functionality
- Provides helpful error messages when no settings are found (only in non-quiet mode)
- Unlike most describe functions, this one explicitly reports when no results are found to help users understand the dual-pattern nature of the command
- The query output includes role name, database name, and settings formatted as newline-separated configuration parameters
- Located in src/bin/psql/describe.c:3761-3829
- Uses LEFT JOINs to handle cases where role or database might be NULL (indicating global settings)

## Simplified Source

```c
bool listDbRoleSettings(const char *pattern, const char *pattern2) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    bool havewhere;

    initPQExpBuffer(&buf);

    // Build SQL query to retrieve role/database settings
    printfPQExpBuffer(&buf,
        "SELECT rolname AS \"%s\", datname AS \"%s\", "
        "pg_catalog.array_to_string(setconfig, E'\\n') AS \"%s\"\n"
        "FROM pg_catalog.pg_db_role_setting s\n"
        "LEFT JOIN pg_catalog.pg_database d ON d.oid = setdatabase\n"
        "LEFT JOIN pg_catalog.pg_roles r ON r.oid = setrole\n",
        gettext_noop("Role"),
        gettext_noop("Database"),
        gettext_noop("Settings"));

    // Add WHERE clauses for pattern matching
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                NULL, "r.rolname", NULL, NULL, &havewhere, 1))
        goto error_return;

    if (!validateSQLNamePattern(&buf, pattern2, havewhere, false,
                                NULL, "d.datname", NULL, NULL, NULL, 1))
        goto error_return;

    appendPQExpBufferStr(&buf, "ORDER BY 1, 2;");

    // Execute the query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    // Display results or helpful error messages
    if (PQntuples(res) == 0 && !pset.quiet) {
        // Provide contextual error messages for no results
        if (pattern && pattern2)
            pg_log_error("Did not find any settings for role \"%s\" and database \"%s\".",
                        pattern, pattern2);
        else if (pattern)
            pg_log_error("Did not find any settings for role \"%s\".", pattern);
        else
            pg_log_error("Did not find any settings.");
    } else {
        // Display the results table
        myopt.title = _("List of settings");
        myopt.translate_header = true;
        printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    }

    PQclear(res);
    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```