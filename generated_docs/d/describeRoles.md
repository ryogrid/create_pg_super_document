# describeRoles

## Location
[src/bin/psql/describe.c:3614-3748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L3614-L3748)

## Overview
Implements the \du and \dg commands in psql to display information about database roles (users and groups).

## Definition

```c
bool
describeRoles(const char *pattern, bool verbose, bool showSystem)
```
## Detailed Description
The  function implements psql's \du (describe users) and \dg (describe groups) commands, which are functionally identical since PostgreSQL treats users and groups as the same entity (roles). The function queries the pg_roles system view to retrieve comprehensive information about database roles and formats it into a readable table.

The function displays role names along with their attributes in a human-readable format. Attributes include superuser status, inheritance capabilities, role creation privileges, database creation privileges, login capabilities, replication permissions, and row-level security bypass permissions (for PostgreSQL 9.5+). It also shows connection limits and password expiration dates when applicable.

In verbose mode, the function includes role descriptions from the system comments. The output is formatted as a table with role names in the first column and a consolidated attributes column that lists all relevant permissions and restrictions for each role.

## Parameters / Member Variables
- `*pattern`: SQL pattern to filter role names (supports wildcards). Schema portions are ignored since roles are cluster-wide objects.
- `verbose`: Boolean flag for verbose mode (\du+ vs \du) - when true, includes role descriptions from pg_description
- `showSystem`: Boolean flag to include system roles (roles starting with 'pg_'). If false, only user-defined roles are shown.
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md): Initialize query buffer for SQL construction
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md): Format SQL query with role attributes
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md): Process and validate the role name pattern
  - [PSQLexec](../P/PSQLexec.md): Execute the role information query
  - [printTableInit](../p/printTableInit.md): Initialize table formatting structure
  - [printTableAddHeader](../p/printTableAddHeader.md): Add column headers for the role table
  - [printTableAddCell](../p/printTableAddCell.md): Add role data to table cells
  - [add_role_attribute](../a/add_role_attribute.md): Helper function to format individual role attributes
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md): Clear buffer for reuse in attribute formatting
  - [printTable](../p/printTable.md): Display the formatted role table
  - [printTableCleanup](../p/printTableCleanup.md): Clean up table formatting resources
  - [pg_malloc0](../p/pg_malloc0.md): Allocate memory for attribute strings
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md): Command dispatcher for both \du and \dg commands in psql

## Notes and Other Information
- The function treats \du and \dg identically since PostgreSQL unified users and groups into roles
- Handles version-specific features like row-level security bypass (PostgreSQL 9.5+)
- Uses internationalization through gettext for translatable attribute names and descriptions
- Implements smart attribute display - only shows relevant attributes for each role (e.g., "Cannot login" instead of showing login capability for non-login roles)
- Connection limits are displayed with proper pluralization using ngettext
- Password expiration dates are shown in a user-friendly format when present
- Memory management includes proper cleanup of dynamically allocated attribute strings
- The function filters out system roles by default unless showSystem is true
- Returns false on SQL errors or validation failures for proper error propagation
- Role descriptions in verbose mode come from the shared object description system (shobj_description)

## Simplified Source

```c
bool describeRoles(const char *pattern, bool verbose, bool showSystem) {
    PQExpBufferData buf;
    PGresult *res;
    printTableContent cont;
    printTableOpt myopt = pset.popt.topt;
    int ncols = 2;
    int nrows = 0;
    int i;
    int conns;
    const char align = 'l';
    char **attr;

    myopt.default_footer = false;
    initPQExpBuffer(&buf);

    // Build query for role information
    printfPQExpBuffer(&buf,
        "SELECT r.rolname, r.rolsuper, r.rolinherit,\n"
        "  r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,\n"
        "  r.rolconnlimit, r.rolvaliduntil");

    // Add description column for verbose mode
    if (verbose) {
        appendPQExpBufferStr(&buf, "\n, pg_catalog.shobj_description(r.oid, 'pg_authid') AS description");
        ncols++;
    }

    // Add replication attribute (always present)
    appendPQExpBufferStr(&buf, "\n, r.rolreplication");

    // Add row-level security bypass for 9.5+
    if (pset.sversion >= 90500) {
        appendPQExpBufferStr(&buf, "\n, r.rolbypassrls");
    }

    appendPQExpBufferStr(&buf, "\nFROM pg_catalog.pg_roles r\n");

    // Filter out system roles unless requested
    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf, "WHERE r.rolname !~ '^pg_'\n");

    // Apply pattern filter
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                NULL, "r.rolname", NULL, NULL, NULL, 1)) {
        termPQExpBuffer(&buf);
        return false;
    }

    // Execute query
    appendPQExpBufferStr(&buf, "ORDER BY 1;");
    res = PSQLexec(buf.data);
    if (!res)
        return false;

    // Initialize table display
    nrows = PQntuples(res);
    attr = pg_malloc0((nrows + 1) * sizeof(*attr));

    printTableInit(&cont, &myopt, "List of roles", ncols, nrows);
    printTableAddHeader(&cont, "Role name", true, align);
    printTableAddHeader(&cont, "Attributes", true, align);

    if (verbose)
        printTableAddHeader(&cont, "Description", true, align);

    // Process each role
    for (i = 0; i < nrows; i++) {
        printTableAddCell(&cont, PQgetvalue(res, i, 0), false, false);

        // Build attributes string
        resetPQExpBuffer(&buf);
        if (strcmp(PQgetvalue(res, i, 1), "t") == 0)
            add_role_attribute(&buf, "Superuser");

        if (strcmp(PQgetvalue(res, i, 2), "t") != 0)
            add_role_attribute(&buf, "No inheritance");

        if (strcmp(PQgetvalue(res, i, 3), "t") == 0)
            add_role_attribute(&buf, "Create role");

        if (strcmp(PQgetvalue(res, i, 4), "t") == 0)
            add_role_attribute(&buf, "Create DB");

        if (strcmp(PQgetvalue(res, i, 5), "t") != 0)
            add_role_attribute(&buf, "Cannot login");

        if (strcmp(PQgetvalue(res, i, (verbose ? 9 : 8)), "t") == 0)
            add_role_attribute(&buf, "Replication");

        if (pset.sversion >= 90500)
            if (strcmp(PQgetvalue(res, i, (verbose ? 10 : 9)), "t") == 0)
                add_role_attribute(&buf, "Bypass RLS");

        // Add connection limit info
        conns = atoi(PQgetvalue(res, i, 6));
        if (conns >= 0) {
            if (buf.len > 0)
                appendPQExpBufferChar(&buf, '\n');

            if (conns == 0)
                appendPQExpBufferStr(&buf, "No connections");
            else
                appendPQExpBuffer(&buf, "%d connection%s", conns, (conns != 1) ? "s" : "");
        }

        // Add password expiration info
        if (strcmp(PQgetvalue(res, i, 7), "") != 0) {
            if (buf.len > 0)
                appendPQExpBufferChar(&buf, '\n');
            appendPQExpBufferStr(&buf, "Password valid until ");
            appendPQExpBufferStr(&buf, PQgetvalue(res, i, 7));
        }

        attr[i] = pg_strdup(buf.data);
        printTableAddCell(&cont, attr[i], false, false);

        if (verbose)
            printTableAddCell(&cont, PQgetvalue(res, i, 8), false, false);
    }

    termPQExpBuffer(&buf);

    // Display and cleanup
    printTable(&cont, pset.queryFout, false, pset.logfile);
    printTableCleanup(&cont);

    for (i = 0; i < nrows; i++)
        free(attr[i]);
    free(attr);

    PQclear(res);
    return true;
}
```