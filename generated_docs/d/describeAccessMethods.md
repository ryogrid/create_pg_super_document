# describeAccessMethods

## Location
[src/bin/psql/describe.c:141-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L141-L214)

## Overview
Implements the \dA psql command to display a list of access methods in the database, supporting both index and table access methods with optional verbose output.

## Definition
```c
bool describeAccessMethods(const char *pattern, bool verbose)
```

## Detailed Description
This function generates and executes a SQL query to list access methods from the pg_am system catalog. It handles PostgreSQL version compatibility by checking for minimum version 9.6 (when access methods were introduced). The function constructs a query that shows access method names and types (Index or Table), with optional verbose information including handler functions and descriptions. It supports pattern-based filtering and provides internationalized output with proper column translation.

## Parameters / Member Variables
- `pattern`: Optional regular expression pattern to filter access methods by name
- `verbose`: Boolean flag to include additional columns (handler function and description)

## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - lengthof
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c:813)

## Notes and Other Information
- Part of psql's describe functionality (\dA command)
- Requires PostgreSQL version 9.6 or later (access methods were introduced in this version)
- Returns early with an error message for unsupported server versions
- Uses static column translation array for proper internationalization
- Distinguishes between index ('i') and table ('t') access method types
- In verbose mode, shows handler function and description from pg_am catalog

## Simplified Source

```c
bool
describeAccessMethods(const char *pattern, bool verbose)
{
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    static const bool translate_columns[] = {false, true, false, false};

    // Check version compatibility - access methods introduced in 9.6
    if (pset.sversion < 90600) {
        char sverbuf[32];
        pg_log_error("The server (version %s) does not support access methods.",
                    formatPGVersionNumber(pset.sversion, false,
                                        sverbuf, sizeof(sverbuf)));
        return true;
    }

    initPQExpBuffer(&buf);

    // Build base query for access methods
    printfPQExpBuffer(&buf,
                      "SELECT amname AS \"%s\",\n"
                      "  CASE amtype"
                      " WHEN 'i' THEN '%s'"
                      " WHEN 't' THEN '%s'"
                      " END AS \"%s\"",
                      gettext_noop("Name"),
                      gettext_noop("Index"),
                      gettext_noop("Table"),
                      gettext_noop("Type"));

    // Add verbose columns if requested
    if (verbose) {
        appendPQExpBuffer(&buf,
                          ",\n  amhandler AS \"%s\",\n"
                          "  pg_catalog.obj_description(oid, 'pg_am') AS \"%s\"",
                          gettext_noop("Handler"),
                          gettext_noop("Description"));
    }

    appendPQExpBufferStr(&buf, "\nFROM pg_catalog.pg_am\n");

    // Apply pattern filtering
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                NULL, "amname", NULL, NULL, NULL, 1)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of access methods");
    myopt.translate_header = true;
    myopt.translate_columns = translate_columns;
    myopt.n_translate_columns = lengthof(translate_columns);

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);
    return true;
}
```