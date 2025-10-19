# listTSTemplates

## Location
[src/bin/psql/describe.c:5459-5523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5459-L5523)

## Overview
Lists PostgreSQL text search templates with optional verbose details including initialization and lexize function information.

## Definition
bool listTSTemplates(const char *pattern, bool verbose)

## Detailed Description
This function implements the \dFt psql command for listing text search templates from the pg_ts_template catalog. It queries template information including schema, name, and description. When verbose mode is enabled, it additionally displays the template's initialization function (tmplinit) and lexize function (tmpllexize) that are used for dictionary creation and text processing. The function supports pattern matching for selective template listing and uses PostgreSQL's visibility rules to show only accessible templates.

## Parameters / Member Variables
- `pattern`: Pattern string for filtering templates by name; if NULL, lists all visible templates  
- `verbose`: Boolean flag to enable verbose output showing initialization and lexize functions

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - gettext_noop
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command processor)

## Notes and Other Information
- Returns false on error, true on success
- Uses pg_ts_template_is_visible function to respect PostgreSQL's visibility rules
- In verbose mode, displays function names as regproc types for better readability
- Templates define the behavior patterns used by text search dictionaries
- Part of psql's text search object inspection functionality
- Results are ordered by schema name, then template name
- Implements internationalization through gettext_noop for column headers
- Supports both simple listing and detailed function inspection modes

## Simplified Source

```c
bool listTSTemplates(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Initialize query buffer
    initPQExpBuffer(&buf);

    // Build query based on verbose mode
    if (verbose) {
        printfPQExpBuffer(&buf,
            "SELECT n.nspname AS \"Schema\", "
            "t.tmplname AS \"Name\", "
            "t.tmplinit::pg_catalog.regproc AS \"Init\", "
            "t.tmpllexize::pg_catalog.regproc AS \"Lexize\", "
            "pg_catalog.obj_description(t.oid, 'pg_ts_template') AS \"Description\"");
    } else {
        printfPQExpBuffer(&buf,
            "SELECT n.nspname AS \"Schema\", "
            "t.tmplname AS \"Name\", "
            "pg_catalog.obj_description(t.oid, 'pg_ts_template') AS \"Description\"");
    }

    // Add FROM clause
    appendPQExpBufferStr(&buf,
        " FROM pg_catalog.pg_ts_template t "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.tmplnamespace");

    // Validate and add pattern matching
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                               "n.nspname", "t.tmplname", NULL,
                               "pg_catalog.pg_ts_template_is_visible(t.oid)",
                               NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    // Add ordering
    appendPQExpBufferStr(&buf, " ORDER BY 1, 2;");

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res) return false;

    // Configure and display results
    myopt.title = "List of text search templates";
    myopt.translate_header = true;
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;
}
```