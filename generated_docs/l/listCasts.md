# listCasts

## Location
[src/bin/psql/describe.c:4790-4907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4790-L4907)

## Overview
The  function implements the  psql command for displaying type cast information in a PostgreSQL database.

## Definition

```c
bool
listCasts(const char *pattern, bool verbose)
```
## Detailed Description
This function queries the  system catalog to retrieve and display information about type casts defined in the database. Type casts define how PostgreSQL can convert values from one data type to another. The function shows the source type, target type, conversion function (if any), and whether the cast is implicit.

The function constructs a complex SQL query that joins multiple system catalogs (, , , , and optionally ) to gather comprehensive cast information. It handles different cast methods:
- Binary coercible casts (no function needed)
- Input/output function casts 
- Function-based casts

The query supports pattern matching on both source and target type names and includes optional verbose output with descriptions.

## Parameters / Member Variables
- `*pattern`: A SQL name pattern (with optional wildcards) to filter which casts to display based on source or target type names. If NULL, all casts are shown.
- `verbose`: If true, includes cast descriptions from the  catalog in the output.
## Dependencies
- Functions called/Symbols referenced:
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Appends formatted text to the buffer
  - : Validates and processes SQL name patterns for both source and target types
  - : Executes the constructed SQL query
  - : Formats and displays the query results with column translation
  - : Cleans up the string buffer
  - : Macro to get array length
- Constants used:
  - : Binary coercible cast method
  - : Input/output function cast method
  - : Explicit cast context
  - : Assignment cast context
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Uses selective column translation for internationalization
- Pattern matching works on both source and target type names (internal and external formats)
- The 'Implicit?' column shows cast context: 'yes' (implicit), 'in assignment', or 'no' (explicit only)
- Function names like '(binary coercible)' and '(with inout)' are not localized to avoid translation conflicts
- Results are ordered by source type and target type names
- Uses error handling with goto for cleanup on validation failures

## Simplified Source

```c
bool listCasts(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    static const bool translate_columns[] = {false, false, false, true, false};

    initPQExpBuffer(&buf);

    // Build query for source and target types
    printfPQExpBuffer(&buf,
        "SELECT pg_catalog.format_type(castsource, NULL) AS \"%s\",\n"
        "       pg_catalog.format_type(casttarget, NULL) AS \"%s\",\n",
        gettext_noop("Source type"),
        gettext_noop("Target type"));

    // Add function/method column
    appendPQExpBuffer(&buf,
        "       CASE WHEN c.castmethod = '%c' THEN '(binary coercible)'\n"
        "            WHEN c.castmethod = '%c' THEN '(with inout)'\n"
        "            ELSE p.proname\n"
        "       END AS \"%s\",\n",
        COERCION_METHOD_BINARY,
        COERCION_METHOD_INOUT,
        gettext_noop("Function"));

    // Add implicit cast column
    appendPQExpBuffer(&buf,
        "       CASE WHEN c.castcontext = '%c' THEN '%s'\n"
        "            WHEN c.castcontext = '%c' THEN '%s'\n"
        "            ELSE '%s'\n"
        "       END AS \"%s\"",
        COERCION_CODE_EXPLICIT,
        gettext_noop("no"),
        COERCION_CODE_ASSIGNMENT,
        gettext_noop("in assignment"),
        gettext_noop("yes"),
        gettext_noop("Implicit?"));

    // Add description column if verbose
    if (verbose)
        appendPQExpBuffer(&buf,
            ",\n       d.description AS \"%s\"",
            gettext_noop("Description"));

    // Add FROM clause with necessary joins
    appendPQExpBufferStr(&buf,
        "\nFROM pg_catalog.pg_cast c LEFT JOIN pg_catalog.pg_proc p\n"
        "     ON c.castfunc = p.oid\n"
        "     LEFT JOIN pg_catalog.pg_type ts\n"
        "     ON c.castsource = ts.oid\n"
        "     LEFT JOIN pg_catalog.pg_namespace ns\n"
        "     ON ns.oid = ts.typnamespace\n"
        "     LEFT JOIN pg_catalog.pg_type tt\n"
        "     ON c.casttarget = tt.oid\n"
        "     LEFT JOIN pg_catalog.pg_namespace nt\n"
        "     ON nt.oid = tt.typnamespace\n");

    // Add description join if verbose
    if (verbose)
        appendPQExpBufferStr(&buf,
            "     LEFT JOIN pg_catalog.pg_description d\n"
            "     ON d.classoid = c.tableoid AND d.objoid = "
            "c.oid AND d.objsubid = 0\n");

    // Add pattern matching for source type
    appendPQExpBufferStr(&buf, "WHERE ( (true");
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                               "ns.nspname", "ts.typname",
                               "pg_catalog.format_type(ts.oid, NULL)",
                               "pg_catalog.pg_type_is_visible(ts.oid)",
                               NULL, 3))
        goto error_return;

    // Add pattern matching for target type
    appendPQExpBufferStr(&buf, ") OR (true");
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                               "nt.nspname", "tt.typname",
                               "pg_catalog.format_type(tt.oid, NULL)",
                               "pg_catalog.pg_type_is_visible(tt.oid)",
                               NULL, 3))
        goto error_return;

    appendPQExpBufferStr(&buf, ") )\nORDER BY 1, 2;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of casts");
    myopt.translate_header = true;
    myopt.translate_columns = translate_columns;
    myopt.n_translate_columns = lengthof(translate_columns);

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```