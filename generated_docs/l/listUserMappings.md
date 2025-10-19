# listUserMappings

## Location
[src/bin/psql/describe.c:5875-5929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5875-L5929)

## Overview
Lists user mappings for foreign servers in the PostgreSQL database, showing which users are mapped to which foreign servers, with optional FDW options.

## Definition
bool listUserMappings(const char *pattern, bool verbose)

## Detailed Description
This function queries the pg_user_mappings system view to display information about user mappings configured for foreign servers. User mappings define how local PostgreSQL users authenticate to remote servers through foreign data wrappers. The function shows the server name and the mapped user name. In verbose mode, it additionally displays the FDW-specific options associated with each mapping, such as authentication credentials or connection parameters, formatted as key-value pairs. This implements the \deu psql command functionality.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter server names (can be NULL to show all user mappings)
- `verbose`: Boolean flag to control whether to show additional detailed information (FDW options)

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
  - gettext_noop
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher)

## Notes and Other Information
- Returns false if pattern validation fails or query execution fails
- Uses the pg_user_mappings view which provides a filtered view of user mappings based on user privileges
- In verbose mode, displays user mapping options using pg_options_to_table() for proper formatting
- Pattern matching applies to both server name and user name fields
- Orders results first by server name, then by user name for consistent output
- Uses internationalization support for all column headers and titles
- The pg_user_mappings view only shows mappings that the current user has privileges to see
- This function corresponds to the \deu command in psql for listing user mappings

## Simplified Source

```c
bool listUserMappings(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Initialize buffer and build base query
    initPQExpBuffer(&buf);
    printfPQExpBuffer(&buf,
        "SELECT um.srvname AS \"%s\",\n"
        "  um.usename AS \"%s\"",
        gettext_noop("Server"),
        gettext_noop("User name"));

    // Add options column if verbose mode
    if (verbose) {
        appendPQExpBuffer(&buf,
            ",\n CASE WHEN umoptions IS NULL THEN '' ELSE "
            "  '(' || pg_catalog.array_to_string(ARRAY(SELECT "
            "  pg_catalog.quote_ident(option_name) ||  ' ' || "
            "  pg_catalog.quote_literal(option_value)  FROM "
            "  pg_catalog.pg_options_to_table(umoptions)),  ', ') || ')' "
            "  END AS \"%s\"",
            gettext_noop("FDW options"));
    }

    // Add FROM clause
    appendPQExpBufferStr(&buf, "\nFROM pg_catalog.pg_user_mappings um\n");

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                NULL, "um.srvname", "um.usename", NULL,
                                NULL, 1)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1, 2;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of user mappings");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    return true;
}
```