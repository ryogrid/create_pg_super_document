# listExtendedStats

## Location
[src/bin/psql/describe.c:4694-4789](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4694-L4789)

## Overview
The  function implements the  psql command for displaying extended statistics objects in a PostgreSQL database.

## Definition

```c
bool
listExtendedStats(const char *pattern)
```
## Detailed Description
This function queries the  system catalog to retrieve and display information about extended statistics objects. Extended statistics are multi-column statistics that help the PostgreSQL query planner make better decisions for complex queries involving correlated columns. The function constructs a SQL query that shows the schema, name, definition, and types of extended statistics (ndistinct, dependencies, and MCV for PostgreSQL 12+).

The function includes version-specific logic to handle differences in PostgreSQL versions:
- Requires PostgreSQL 10.0+ (extended statistics were introduced in version 10)
- Uses different column definition queries for PostgreSQL 14+ vs earlier versions
- Includes MCV (Most Common Values) statistics for PostgreSQL 12+

## Parameters / Member Variables
- `*pattern`: A SQL name pattern (with optional wildcards) to filter which extended statistics to display. If NULL, all visible extended statistics are shown.
## Dependencies
- Functions called/Symbols referenced:
  - : Formats PostgreSQL version numbers for display
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Appends formatted text to the buffer
  - : Validates and processes SQL name patterns with wildcards
  - : Executes the constructed SQL query
  - : Formats and displays the query results
  - : Cleans up the string buffer
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Displays an error message and returns  (non-fatal) if the server version is too old
- The output includes schema, name, definition, and availability of different statistic types
- Uses internationalization (gettext) for column headers
- Respects object visibility rules through 
- Results are ordered by schema name and object name

## Simplified Source

```c
bool listExtendedStats(const char *pattern) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Check if server supports extended statistics (PostgreSQL 10+)
    if (pset.sversion < 100000) {
        char sverbuf[32];
        pg_log_error("The server (version %s) does not support extended statistics.",
                    formatPGVersionNumber(pset.sversion, false, sverbuf, sizeof(sverbuf)));
        return true;
    }

    initPQExpBuffer(&buf);

    // Build base query for schema and name
    printfPQExpBuffer(&buf,
        "SELECT \n"
        "es.stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS \"%s\", \n"
        "es.stxname AS \"%s\", \n",
        gettext_noop("Schema"),
        gettext_noop("Name"));

    // Add definition column with version-specific logic
    if (pset.sversion >= 140000) {
        // PostgreSQL 14+ uses pg_get_statisticsobjdef_columns
        appendPQExpBuffer(&buf,
            "pg_catalog.format('%%s FROM %%s', \n"
            "  pg_catalog.pg_get_statisticsobjdef_columns(es.oid), \n"
            "  es.stxrelid::pg_catalog.regclass) AS \"%s\"",
            gettext_noop("Definition"));
    } else {
        // Earlier versions build column list manually
        appendPQExpBuffer(&buf,
            "pg_catalog.format('%%s FROM %%s', \n"
            "  (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(a.attname),', ') \n"
            "   FROM pg_catalog.unnest(es.stxkeys) s(attnum) \n"
            "   JOIN pg_catalog.pg_attribute a \n"
            "   ON (es.stxrelid = a.attrelid \n"
            "   AND a.attnum = s.attnum \n"
            "   AND NOT a.attisdropped)), \n"
            "es.stxrelid::pg_catalog.regclass) AS \"%s\"",
            gettext_noop("Definition"));
    }

    // Add statistic type columns
    appendPQExpBuffer(&buf,
        ",\nCASE WHEN 'd' = any(es.stxkind) THEN 'defined' \n"
        "END AS \"%s\", \n"
        "CASE WHEN 'f' = any(es.stxkind) THEN 'defined' \n"
        "END AS \"%s\"",
        gettext_noop("Ndistinct"),
        gettext_noop("Dependencies"));

    // Add MCV column for PostgreSQL 12+
    if (pset.sversion >= 120000) {
        appendPQExpBuffer(&buf,
            ",\nCASE WHEN 'm' = any(es.stxkind) THEN 'defined' \n"
            "END AS \"%s\" ",
            gettext_noop("MCV"));
    }

    appendPQExpBufferStr(&buf, " \nFROM pg_catalog.pg_statistic_ext es \n");

    // Add pattern filtering
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                               "es.stxnamespace::pg_catalog.regnamespace::pg_catalog.text",
                               "es.stxname", NULL,
                               "pg_catalog.pg_statistics_obj_is_visible(es.oid)",
                               NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1, 2;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of extended statistics");
    myopt.translate_header = true;
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;
}
```