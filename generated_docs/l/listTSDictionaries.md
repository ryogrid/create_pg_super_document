# listTSDictionaries

## Location
[src/bin/psql/describe.c:5394-5458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5394-L5458)

## Overview
Lists PostgreSQL text search dictionaries with optional verbose details including template information and initialization options.

## Definition
bool listTSDictionaries(const char *pattern, bool verbose)

## Detailed Description
This function implements the \dFd psql command for listing text search dictionaries from the pg_ts_dict catalog. It queries dictionary information including schema, name, and description. When verbose mode is enabled, it additionally displays the associated template name (with namespace) and dictionary initialization options. The function supports pattern matching for selective dictionary listing and uses PostgreSQL's visibility rules to show only accessible dictionaries.

## Parameters / Member Variables
- `pattern`: Pattern string for filtering dictionaries by name; if NULL, lists all visible dictionaries
- `verbose`: Boolean flag to enable verbose output showing template and initialization options

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
  - [exec_command_d](../e/exec_command_d.md) (psql command processor)

## Notes and Other Information
- Returns false on error, true on success
- Uses pg_ts_dict_is_visible function to respect PostgreSQL's visibility rules
- In verbose mode, joins with pg_ts_template and pg_namespace to show complete template information
- Handles null namespace gracefully by displaying '(null)' for system templates
- Part of psql's text search object inspection functionality
- Results are ordered by schema name, then dictionary name
- Implements internationalization through gettext_noop for column headers

## Simplified Source

```c
bool listTSDictionaries(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Initialize query buffer
    initPQExpBuffer(&buf);

    // Build basic SELECT query for schema and dictionary name
    printfPQExpBuffer(&buf,
        "SELECT n.nspname AS \"Schema\", d.dictname AS \"Name\",");

    // Add template and init options in verbose mode
    if (verbose) {
        appendPQExpBuffer(&buf,
            " (SELECT COALESCE(nt.nspname, '(null)')::pg_catalog.text || '.' || t.tmplname "
            "FROM pg_catalog.pg_ts_template t "
            "LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid = t.tmplnamespace "
            "WHERE d.dicttemplate = t.oid) AS \"Template\", "
            "d.dictinitoption AS \"Init options\",");
    }

    // Add description column
    appendPQExpBuffer(&buf,
        " pg_catalog.obj_description(d.oid, 'pg_ts_dict') AS \"Description\"");

    // Add FROM clause
    appendPQExpBufferStr(&buf,
        " FROM pg_catalog.pg_ts_dict d "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = d.dictnamespace");

    // Validate and add pattern matching
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                               "n.nspname", "d.dictname", NULL,
                               "pg_catalog.pg_ts_dict_is_visible(d.oid)",
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
    myopt.title = "List of text search dictionaries";
    myopt.translate_header = true;
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;
}
```