# listTables

## Location
[src/bin/psql/describe.c:3909-4106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L3909-L4106)

## Overview
A comprehensive psql command function that implements multiple table-related metacommands (\\dt, \\di, \\dv, etc.) to display various types of database relations including tables, indexes, views, sequences, and foreign tables.

## Definition

```c
bool
listTables(const char *tabtypes, const char *pattern, bool verbose, bool showSystem)
```
## Detailed Description
This function is the primary handler for multiple psql metacommands that list database relations. It supports listing tables (\\dt), indexes (\\di), views (\\dv), materialized views (\\dm), sequences (\\ds), and foreign tables (\\dE) either individually or in combination. The tabtypes parameter determines which relation types to include using single character codes (t=tables, i=indexes, v=views, m=materialized views, s=sequences, E=foreign tables). The function constructs a complex SQL query that joins pg_class with pg_namespace and optionally with pg_am (access methods) and pg_index depending on the requested information. It provides detailed information including schema, name, type, owner, and optionally persistence, access method, size, and description.

## Parameters / Member Variables
- `*tabtypes`: A string containing characters specifying which relation types to display ('t'=tables, 'i'=indexes, 'v'=views, 'm'=materialized views, 's'=sequences, 'E'=foreign tables)
- `*pattern`: A SQL pattern (with wildcards) to filter by relation name, or NULL to match all relations
- `verbose`: Boolean flag to include additional columns like persistence, access method, size, and description
- `showSystem`: Boolean flag indicating whether to include system relations (catalog tables, toast tables, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (PostgreSQL's expandable string buffer structure)
  - [printQueryOpt](../p/printQueryOpt.md) (print formatting options structure)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formatted append to buffer)
  - RELKIND_* constants (relation kind constants like RELKIND_RELATION, RELKIND_VIEW, etc.)
  - CppAsString2 (macro to convert constants to strings)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and append SQL name patterns)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup buffer)
  - [PSQLexec](../P/PSQLexec.md) (execute SQL query)
  - lengthof (macro to get array length)
  - [printQuery](../p/printQuery.md) (display query results)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher at src/bin/psql/command.c:800, 931)
  - DESCRIBE_H (function declaration in src/bin/psql/describe.h:71)

## Notes and Other Information
- Returns true on success, false on error
- Implements multiple psql metacommands: \\dt, \\di, \\dv, \\dm, \\ds, \\dE
- If tabtypes is empty, defaults to showing tables, views, materialized views, sequences, and foreign tables
- Version-aware: includes access method information for PostgreSQL 12.0+ when not hidden
- In verbose mode, shows additional columns including persistence (permanent/temporary/unlogged), access method (if applicable), size, and description
- By default excludes system schemas (pg_catalog, pg_toast, information_schema) unless showSystem is true or a pattern is specified
- Supports TOAST table visibility when showSystem is true or a pattern is provided
- Provides helpful error messages when no relations are found (only in non-quiet mode)
- Uses column translation for internationalization support
- Results are ordered by schema name and relation name for consistent display
- Located in src/bin/psql/describe.c:3909-4106

## Simplified Source

```c
bool listTables(const char *tabtypes, const char *pattern, bool verbose, bool showSystem) {
    // Parse which relation types to show
    bool showTables = strchr(tabtypes, 't') != NULL;
    bool showIndexes = strchr(tabtypes, 'i') != NULL;
    bool showViews = strchr(tabtypes, 'v') != NULL;
    bool showMatViews = strchr(tabtypes, 'm') != NULL;
    bool showSeq = strchr(tabtypes, 's') != NULL;
    bool showForeign = strchr(tabtypes, 'E') != NULL;

    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    int cols_so_far;
    bool translate_columns[] = {false, false, true, false, false, false, false, false, false};

    // Default to showing all main relation types if none specified
    if (!(showTables || showIndexes || showViews || showMatViews || showSeq || showForeign))
        showTables = showViews = showMatViews = showSeq = showForeign = true;

    initPQExpBuffer(&buf);

    // Build base query selecting schema, name, type, owner
    printfPQExpBuffer(&buf,
        "SELECT n.nspname as \"%s\",\n"
        "  c.relname as \"%s\",\n"
        "  CASE c.relkind"
        " WHEN 'r' THEN '%s'"  // table
        " WHEN 'v' THEN '%s'"  // view
        " WHEN 'm' THEN '%s'"  // materialized view
        " WHEN 'i' THEN '%s'"  // index
        " WHEN 'S' THEN '%s'"  // sequence
        " WHEN 't' THEN '%s'"  // TOAST table
        " WHEN 'f' THEN '%s'"  // foreign table
        " WHEN 'p' THEN '%s'"  // partitioned table
        " WHEN 'I' THEN '%s'"  // partitioned index
        " END as \"%s\",\n"
        "  pg_catalog.pg_get_userbyid(c.relowner) as \"%s\"",
        gettext_noop("Schema"), gettext_noop("Name"),
        gettext_noop("table"), gettext_noop("view"), gettext_noop("materialized view"),
        gettext_noop("index"), gettext_noop("sequence"), gettext_noop("TOAST table"),
        gettext_noop("foreign table"), gettext_noop("partitioned table"),
        gettext_noop("partitioned index"), gettext_noop("Type"), gettext_noop("Owner"));
    cols_so_far = 4;

    // Add table name for indexes
    if (showIndexes) {
        appendPQExpBuffer(&buf, ",\n  c2.relname as \"%s\"", gettext_noop("Table"));
        cols_so_far++;
    }

    // Add verbose columns if requested
    if (verbose) {
        // Show persistence (permanent/temporary/unlogged)
        appendPQExpBuffer(&buf,
            ",\n  CASE c.relpersistence WHEN 'p' THEN '%s' WHEN 't' THEN '%s' WHEN 'u' THEN '%s' END as \"%s\"",
            gettext_noop("permanent"), gettext_noop("temporary"), gettext_noop("unlogged"),
            gettext_noop("Persistence"));
        translate_columns[cols_so_far] = true;

        // Add access method for PostgreSQL 12+
        if (pset.sversion >= 120000 && !pset.hide_tableam &&
            (showTables || showMatViews || showIndexes))
            appendPQExpBuffer(&buf, ",\n  am.amname as \"%s\"", gettext_noop("Access method"));

        // Add size and description
        appendPQExpBuffer(&buf,
            ",\n  pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as \"%s\""
            ",\n  pg_catalog.obj_description(c.oid, 'pg_class') as \"%s\"",
            gettext_noop("Size"), gettext_noop("Description"));
    }

    // Add FROM clause with joins
    appendPQExpBufferStr(&buf,
        "\nFROM pg_catalog.pg_class c"
        "\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace");

    // Join access method table if needed
    if (pset.sversion >= 120000 && !pset.hide_tableam &&
        (showTables || showMatViews || showIndexes))
        appendPQExpBufferStr(&buf, "\n     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam");

    // Join index tables if showing indexes
    if (showIndexes)
        appendPQExpBufferStr(&buf,
            "\n     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid"
            "\n     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid");

    // Build WHERE clause for relation kinds
    appendPQExpBufferStr(&buf, "\nWHERE c.relkind IN (");
    if (showTables) {
        appendPQExpBufferStr(&buf, "'r','p',");  // relations, partitioned tables
        if (showSystem || pattern)
            appendPQExpBufferStr(&buf, "'t',");  // TOAST tables
    }
    if (showViews) appendPQExpBufferStr(&buf, "'v',");
    if (showMatViews) appendPQExpBufferStr(&buf, "'m',");
    if (showIndexes) appendPQExpBufferStr(&buf, "'i','I',");
    if (showSeq) appendPQExpBufferStr(&buf, "'S',");
    if (showSystem || pattern) appendPQExpBufferStr(&buf, "'s',");  // special
    if (showForeign) appendPQExpBufferStr(&buf, "'f',");
    appendPQExpBufferStr(&buf, "'')");  // dummy

    // Filter out system schemas unless requested
    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf,
            "      AND n.nspname <> 'pg_catalog'\n"
            "      AND n.nspname !~ '^pg_toast'\n"
            "      AND n.nspname <> 'information_schema'\n");

    // Add pattern matching
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                                "n.nspname", "c.relname", NULL,
                                "pg_catalog.pg_table_is_visible(c.oid)",
                                NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1,2;");

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    // Handle empty results
    if (PQntuples(res) == 0 && !pset.quiet) {
        if (pattern)
            pg_log_error("Did not find any relation named \"%s\".", pattern);
        else
            pg_log_error("Did not find any relations.");
    } else {
        // Display results
        myopt.title = _("List of relations");
        myopt.translate_header = true;
        myopt.translate_columns = translate_columns;
        myopt.n_translate_columns = lengthof(translate_columns);

        printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    }

    PQclear(res);
    return true;
}
```