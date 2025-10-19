# describeTablespaces

## Location
[src/bin/psql/describe.c:215-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L215-L287)

## Overview
Implements the \db psql command to display a list of tablespaces in the database, showing their names, owners, locations, and optional detailed information.

## Definition
```c
bool describeTablespaces(const char *pattern, bool verbose)
```

## Detailed Description
This function generates and executes a SQL query to list tablespaces from the pg_tablespace system catalog. It constructs a query that displays essential tablespace information including name, owner (resolved via pg_get_userbyid), and physical location (via pg_tablespace_location). In verbose mode, it additionally shows access control lists, tablespace options, size information (formatted with pg_size_pretty), and descriptions. The function supports pattern-based filtering and provides proper internationalization for column headers.

## Parameters / Member Variables
- `pattern`: Optional regular expression pattern to filter tablespaces by name
- `verbose`: Boolean flag to include additional columns (ACL, options, size, description)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c:839)

## Notes and Other Information
- Part of psql's describe functionality (\db command)
- Uses PostgreSQL system functions for user resolution (pg_get_userbyid) and location lookup (pg_tablespace_location)
- In verbose mode, displays human-readable size information using pg_size_pretty
- Shows access control information through printACLColumn function
- Retrieves shared object descriptions from the pg_tablespace catalog
- Orders results alphabetically by tablespace name
- Returns boolean indicating success/failure of the operation

## Simplified Source

```c
bool
describeTablespaces(const char *pattern, bool verbose)
{
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    initPQExpBuffer(&buf);

    // Build base query for tablespaces
    printfPQExpBuffer(&buf,
                      "SELECT spcname AS \"%s\",\n"
                      "  pg_catalog.pg_get_userbyid(spcowner) AS \"%s\",\n"
                      "  pg_catalog.pg_tablespace_location(oid) AS \"%s\"",
                      gettext_noop("Name"),
                      gettext_noop("Owner"),
                      gettext_noop("Location"));

    // Add verbose columns if requested
    if (verbose) {
        appendPQExpBufferStr(&buf, ",\n  ");
        printACLColumn(&buf, "spcacl");
        appendPQExpBuffer(&buf,
                          ",\n  spcoptions AS \"%s\""
                          ",\n  pg_catalog.pg_size_pretty(pg_catalog.pg_tablespace_size(oid)) AS \"%s\""
                          ",\n  pg_catalog.shobj_description(oid, 'pg_tablespace') AS \"%s\"",
                          gettext_noop("Options"),
                          gettext_noop("Size"),
                          gettext_noop("Description"));
    }

    appendPQExpBufferStr(&buf, "\nFROM pg_catalog.pg_tablespace\n");

    // Apply pattern filtering
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                NULL, "spcname", NULL, NULL, NULL, 1)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of tablespaces");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);
    return true;
}
```