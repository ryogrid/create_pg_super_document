# listPublications

## Location
[src/bin/psql/describe.c:6217-6292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6217-L6292)

## Overview
Lists PostgreSQL logical replication publications, displaying their properties such as name, owner, and replication settings.

## Definition

```c
bool
listPublications(const char *pattern)
```
## Detailed Description
The  function implements the  psql meta-command functionality to display information about logical replication publications. It constructs and executes a SQL query against the  system catalog to retrieve publication details. The function supports optional pattern matching to filter results and adapts its output columns based on the PostgreSQL server version to show version-appropriate features.

The function performs several key operations:
1. Version checking to ensure the server supports publications (PostgreSQL 10.0+)
2. Dynamic SQL query construction with version-specific columns
3. Pattern validation and filtering
4. Result formatting and display using psql's standard table output format

## Parameters / Member Variables
- `*pattern`: Optional regular expression pattern to filter publications by name. If NULL, all publications are listed.
## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - lengthof
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c for \dRp command processing)

## Notes and Other Information
- Requires PostgreSQL 10.0 or later (publications were introduced in version 10)
- Dynamically adjusts column display based on server version:
  - PostgreSQL 11+: Includes 'Truncates' column (pubtruncate)
  - PostgreSQL 13+: Includes 'Via root' column (pubviaroot)
- Uses psql's standard query result formatting with internationalization support
- Returns boolean indicating success/failure of the operation
- Part of psql's describe.c module which handles various \d commands

## Simplified Source

```c
bool listPublications(const char *pattern) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    static const bool translate_columns[] = {false, false, false, false, false, false, false, false};

    // Check server version for publication support
    if (pset.sversion < 100000) {
        char sverbuf[32];
        pg_log_error("The server (version %s) does not support publications.",
                     formatPGVersionNumber(pset.sversion, false,
                                           sverbuf, sizeof(sverbuf)));
        return true;
    }

    // Build base query
    initPQExpBuffer(&buf);
    printfPQExpBuffer(&buf,
        "SELECT pubname AS \"%s\",\n"
        "  pg_catalog.pg_get_userbyid(pubowner) AS \"%s\",\n"
        "  puballtables AS \"%s\",\n"
        "  pubinsert AS \"%s\",\n"
        "  pubupdate AS \"%s\",\n"
        "  pubdelete AS \"%s\"",
        gettext_noop("Name"),
        gettext_noop("Owner"),
        gettext_noop("All tables"),
        gettext_noop("Inserts"),
        gettext_noop("Updates"),
        gettext_noop("Deletes"));

    // Add version-specific columns
    if (pset.sversion >= 110000) {
        appendPQExpBuffer(&buf,
            ",\n  pubtruncate AS \"%s\"",
            gettext_noop("Truncates"));
    }
    if (pset.sversion >= 130000) {
        appendPQExpBuffer(&buf,
            ",\n  pubviaroot AS \"%s\"",
            gettext_noop("Via root"));
    }

    appendPQExpBufferStr(&buf, "\nFROM pg_catalog.pg_publication\n");

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                NULL, "pubname", NULL,
                                NULL,
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

    myopt.title = _("List of publications");
    myopt.translate_header = true;
    myopt.translate_columns = translate_columns;
    myopt.n_translate_columns = lengthof(translate_columns);

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    return true;
}
```