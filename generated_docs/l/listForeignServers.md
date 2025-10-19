# listForeignServers

## Location
[src/bin/psql/describe.c:5799-5874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5799-L5874)

## Overview
Lists foreign servers in the PostgreSQL database, showing their names, owners, associated foreign data wrappers, and optionally detailed configuration information.

## Definition
bool listForeignServers(const char *pattern, bool verbose)

## Detailed Description
This function queries the pg_foreign_server and related system catalogs to display information about foreign servers configured in the database. It shows basic information including server name, owner, and the foreign data wrapper used. In verbose mode, it additionally displays access control lists (ACLs), server type, version, server-specific options formatted as key-value pairs, and descriptions. The function joins pg_foreign_server with pg_foreign_data_wrapper to show the relationship between servers and their underlying FDWs. This implements the \des psql command functionality.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter foreign server names (can be NULL to show all servers)
- `verbose`: Boolean flag to control whether to show additional detailed information (ACLs, type, version, options, descriptions)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
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
- Performs JOIN between pg_foreign_server and pg_foreign_data_wrapper tables to show FDW relationships
- In verbose mode, displays server options using pg_options_to_table() for proper formatting
- Includes left join with pg_description for object descriptions in verbose mode
- Orders results alphabetically by server name for consistent output
- Uses internationalization support for all column headers and titles
- The server type and version fields are optional and may be NULL
- This function corresponds to the \des command in psql for listing foreign servers

## Simplified Source

```c
bool listForeignServers(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Initialize buffer and build base query
    initPQExpBuffer(&buf);
    printfPQExpBuffer(&buf,
        "SELECT s.srvname AS \"%s\",\n"
        "  pg_catalog.pg_get_userbyid(s.srvowner) AS \"%s\",\n"
        "  f.fdwname AS \"%s\"",
        gettext_noop("Name"),
        gettext_noop("Owner"),
        gettext_noop("Foreign-data wrapper"));

    // Add verbose columns if requested
    if (verbose) {
        appendPQExpBufferStr(&buf, ",\n  ");
        printACLColumn(&buf, "s.srvacl");
        appendPQExpBuffer(&buf,
            ",\n"
            "  s.srvtype AS \"%s\",\n"
            "  s.srvversion AS \"%s\",\n"
            "  CASE WHEN srvoptions IS NULL THEN '' ELSE "
            "  '(' || pg_catalog.array_to_string(ARRAY(SELECT "
            "  pg_catalog.quote_ident(option_name) ||  ' ' || "
            "  pg_catalog.quote_literal(option_value)  FROM "
            "  pg_catalog.pg_options_to_table(srvoptions)),  ', ') || ')' "
            "  END AS \"%s\",\n"
            "  d.description AS \"%s\"",
            gettext_noop("Type"),
            gettext_noop("Version"),
            gettext_noop("FDW options"),
            gettext_noop("Description"));
    }

    // Add FROM clause with JOIN to foreign data wrapper
    appendPQExpBufferStr(&buf,
        "\nFROM pg_catalog.pg_foreign_server s\n"
        "     JOIN pg_catalog.pg_foreign_data_wrapper f ON f.oid=s.srvfdw\n");

    // Add description join for verbose mode
    if (verbose) {
        appendPQExpBufferStr(&buf,
            "LEFT JOIN pg_catalog.pg_description d\n       "
            "ON d.classoid = s.tableoid AND d.objoid = s.oid "
            "AND d.objsubid = 0\n");
    }

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                NULL, "s.srvname", NULL, NULL,
                                NULL, 1)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of foreign servers");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    return true;
}
```