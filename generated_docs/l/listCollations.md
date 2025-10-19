# listCollations

## Location
[src/bin/psql/describe.c:4908-5025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4908-L5025)

## Overview
The  function implements the  psql command for displaying collation information in a PostgreSQL database.

## Definition

```c
bool
listCollations(const char *pattern, bool verbose, bool showSystem)
```
## Detailed Description
This function queries the  system catalog to retrieve and display information about collations defined in the database. Collations define the rules for sorting and comparing text data in different languages and locales. The function shows comprehensive collation information including schema, name, provider, locale settings, ICU rules, and deterministic properties.

The function includes extensive version-specific logic to handle the evolution of collation features across PostgreSQL versions:
- Provider information handling (PostgreSQL 10+)
- Locale column naming changes (PostgreSQL 15+ uses , PostgreSQL 17+ uses )
- ICU rules support (PostgreSQL 16+)
- Deterministic collation support (PostgreSQL 12+)

The query filters collations based on the current database encoding to show only usable collations and can optionally exclude system collations.

## Parameters / Member Variables
- `*pattern`: A SQL name pattern (with optional wildcards) to filter which collations to display. If NULL, all visible collations are shown.
- `verbose`: If true, includes collation descriptions from the  catalog in the output.
- `showSystem`: If true, includes system collations from  and  schemas; if false, excludes them (unless a pattern is specified).
## Dependencies
- Functions called/Symbols referenced:
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Appends formatted text to the buffer
  - : Validates and processes SQL name patterns with wildcards
  - : Executes the constructed SQL query
  - : Formats and displays the query results with column translation
  - : Cleans up the string buffer
  - : Macro to get array length
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Uses selective column translation for internationalization
- Automatically filters out collations that are incompatible with the current database encoding
- Provider types include: 'default', 'builtin', 'libc', and 'icu' (PostgreSQL 10+)
- Shows different locale information based on PostgreSQL version and provider type
- ICU Rules column shows custom ICU sorting rules for ICU collations (PostgreSQL 16+)
- Deterministic property indicates whether the collation provides consistent, reproducible results
- Results are ordered by schema name and collation name
- System collation filtering respects the pattern parameter - if a pattern is provided, system collations may still be shown if they match

## Simplified Source

```c
bool listCollations(const char *pattern, bool verbose, bool showSystem) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Initialize query buffer
    initPQExpBuffer(&buf);

    // Build SELECT query with schema and collation name
    printfPQExpBuffer(&buf,
        "SELECT n.nspname AS \"Schema\", c.collname AS \"Name\",");

    // Add provider column based on PostgreSQL version
    if (pset.sversion >= 100000) {
        appendPQExpBuffer(&buf,
            "CASE c.collprovider WHEN 'd' THEN 'default' "
            "WHEN 'b' THEN 'builtin' WHEN 'c' THEN 'libc' "
            "WHEN 'i' THEN 'icu' END AS \"Provider\",");
    } else {
        appendPQExpBuffer(&buf, "'libc' AS \"Provider\",");
    }

    // Add collate and ctype columns
    appendPQExpBuffer(&buf,
        "c.collcollate AS \"Collate\", c.collctype AS \"Ctype\",");

    // Add locale column (name varies by version)
    if (pset.sversion >= 170000) {
        appendPQExpBuffer(&buf, "c.colllocale AS \"Locale\",");
    } else if (pset.sversion >= 150000) {
        appendPQExpBuffer(&buf, "c.colliculocale AS \"Locale\",");
    } else {
        appendPQExpBuffer(&buf, "c.collcollate AS \"Locale\",");
    }

    // Add ICU rules column (PostgreSQL 16+)
    if (pset.sversion >= 160000) {
        appendPQExpBuffer(&buf, "c.collicurules AS \"ICU Rules\",");
    } else {
        appendPQExpBuffer(&buf, "NULL AS \"ICU Rules\",");
    }

    // Add deterministic column (PostgreSQL 12+)
    if (pset.sversion >= 120000) {
        appendPQExpBuffer(&buf,
            "CASE WHEN c.collisdeterministic THEN 'yes' ELSE 'no' END AS \"Deterministic?\"");
    } else {
        appendPQExpBuffer(&buf, "'yes' AS \"Deterministic?\"");
    }

    // Add description if verbose mode
    if (verbose) {
        appendPQExpBuffer(&buf,
            ", pg_catalog.obj_description(c.oid, 'pg_collation') AS \"Description\"");
    }

    // Add FROM clause and basic WHERE conditions
    appendPQExpBufferStr(&buf,
        " FROM pg_catalog.pg_collation c, pg_catalog.pg_namespace n "
        "WHERE n.oid = c.collnamespace");

    // Filter system schemas unless explicitly requested
    if (!showSystem && !pattern) {
        appendPQExpBufferStr(&buf,
            " AND n.nspname <> 'pg_catalog' "
            "AND n.nspname <> 'information_schema'");
    }

    // Filter by database encoding compatibility
    appendPQExpBufferStr(&buf,
        " AND c.collencoding IN (-1, pg_catalog.pg_char_to_encoding("
        "pg_catalog.getdatabaseencoding()))");

    // Validate and add pattern matching
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                               "n.nspname", "c.collname", NULL,
                               "pg_catalog.pg_collation_is_visible(c.oid)",
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
    myopt.title = "List of collations";
    myopt.translate_header = true;
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;
}
```