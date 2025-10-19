# listPartitionedTables

## Location
[src/bin/psql/describe.c:4107-4306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4107-L4306)

## Overview
A specialized psql command function that implements the \\dP metacommand to display partitioned tables and indexes with detailed partition hierarchy information and size statistics.

## Definition

```c
bool
listPartitionedTables(const char *reltypes, const char *pattern, bool verbose)
```
## Detailed Description
This function provides functionality for the psql \\dP metacommand, which is specifically designed to list partitioned tables and indexes introduced in PostgreSQL 10.0 with declarative partitioning. The function supports filtering by relation types (tables 't', indexes 'i', and nested partitions 'n') and can display comprehensive information about partition hierarchies. In verbose mode, it shows partition sizes using either recursive queries (pre-12.0) or the pg_partition_tree function (12.0+). The function handles mixed output when both tables and indexes are requested and provides parent-child relationship information when nested partitions or patterns are specified.

## Parameters / Member Variables
- `*reltypes`: A string containing characters specifying which types to display ('t'=tables, 'i'=indexes, 'n'=nested/non-leaf partitioned tables)
- `*pattern`: A SQL pattern (with wildcards) to filter by relation name, or NULL to match all partitioned relations
- `verbose`: Boolean flag to include additional columns like partition sizes and description
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (PostgreSQL's expandable string buffer structure)
  - [printQueryOpt](../p/printQueryOpt.md) (print formatting options structure)  
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md) (format PostgreSQL version for display)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formatted append to buffer)
  - CppAsString2 (macro to convert constants to strings)
  - RELKIND_PARTITIONED_TABLE, RELKIND_PARTITIONED_INDEX (relation kind constants)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and append SQL name patterns)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup buffer)
  - [PSQLexec](../P/PSQLexec.md) (execute SQL query)
  - lengthof (macro to get array length)
  - [printQuery](../p/printQuery.md) (display query results)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher at src/bin/psql/command.c:914)
  - DESCRIBE_H (function declaration in src/bin/psql/describe.h:74)

## Notes and Other Information
- Returns true on success, false on error
- Implements the psql \\dP metacommand functionality
- Requires PostgreSQL 10.0+ (exits early with error message for older versions)
- If no relation types are specified, defaults to showing both tables and indexes
- Supports multiple output titles: "List of partitioned tables", "List of partitioned indexes", or "List of partitioned relations"
- Version-aware size calculation: uses recursive CTE for pre-12.0, pg_partition_tree function for 12.0+
- In verbose mode with nested partitions, shows both "Leaf partition size" (direct children) and "Total size" (all descendants)
- Shows parent-child relationships when 'n' flag is used or pattern is specified
- By default excludes system schemas unless a pattern is provided
- Automatically filters out leaf partitions unless nested viewing or pattern matching is requested
- Results are ordered by schema, type (if mixed output), parent name, and relation name
- Uses column translation for internationalization support
- Located in src/bin/psql/describe.c:4107-4306

## Simplified Source

```c
bool listPartitionedTables(const char *reltypes, const char *pattern, bool verbose) {
    // Parse relation type flags
    bool showTables = strchr(reltypes, 't') != NULL;
    bool showIndexes = strchr(reltypes, 'i') != NULL;
    bool showNested = strchr(reltypes, 'n') != NULL;

    PQExpBufferData buf;
    PQExpBufferData title;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    bool translate_columns[] = {false, false, false, false, false, false, false, false, false};
    const char *tabletitle;
    bool mixed_output = false;

    // Check PostgreSQL version (partitioning requires 10.0+)
    if (pset.sversion < 100000) {
        char sverbuf[32];
        pg_log_error("The server (version %s) does not support declarative table partitioning.",
                     formatPGVersionNumber(pset.sversion, false, sverbuf, sizeof(sverbuf)));
        return true;
    }

    // Default to showing both tables and indexes if none specified
    if (!showTables && !showIndexes)
        showTables = showIndexes = true;

    // Determine appropriate title
    if (showIndexes && !showTables)
        tabletitle = _("List of partitioned indexes");
    else if (showTables && !showIndexes)
        tabletitle = _("List of partitioned tables");
    else {
        tabletitle = _("List of partitioned relations");
        mixed_output = true;
    }

    initPQExpBuffer(&buf);

    // Build base query
    printfPQExpBuffer(&buf,
        "SELECT n.nspname as \"%s\",\n"
        "  c.relname as \"%s\",\n"
        "  pg_catalog.pg_get_userbyid(c.relowner) as \"%s\"",
        gettext_noop("Schema"), gettext_noop("Name"), gettext_noop("Owner"));

    // Add type column for mixed output
    if (mixed_output) {
        appendPQExpBuffer(&buf,
            ",\n  CASE c.relkind"
            " WHEN 'p' THEN '%s'"  // partitioned table
            " WHEN 'I' THEN '%s'"  // partitioned index
            " END as \"%s\"",
            gettext_noop("partitioned table"), gettext_noop("partitioned index"),
            gettext_noop("Type"));
        translate_columns[3] = true;
    }

    // Add parent name for nested view or pattern matching
    if (showNested || pattern)
        appendPQExpBuffer(&buf,
            ",\n  inh.inhparent::pg_catalog.regclass as \"%s\"",
            gettext_noop("Parent name"));

    // Add table name for indexes
    if (showIndexes)
        appendPQExpBuffer(&buf,
            ",\n c2.oid::pg_catalog.regclass as \"%s\"",
            gettext_noop("Table"));

    // Add verbose columns (sizes and description)
    if (verbose) {
        if (showNested) {
            // Show both direct and total sizes for nested view
            appendPQExpBuffer(&buf,
                ",\n  s.dps as \"%s\"",
                gettext_noop("Leaf partition size"));
            appendPQExpBuffer(&buf,
                ",\n  s.tps as \"%s\"",
                gettext_noop("Total size"));
        } else {
            // Show only total size
            appendPQExpBuffer(&buf,
                ",\n  s.tps as \"%s\"",
                gettext_noop("Total size"));
        }

        appendPQExpBuffer(&buf,
            ",\n  pg_catalog.obj_description(c.oid, 'pg_class') as \"%s\"",
            gettext_noop("Description"));
    }

    // Add FROM clause and joins
    appendPQExpBufferStr(&buf,
        "\nFROM pg_catalog.pg_class c"
        "\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace");

    if (showIndexes)
        appendPQExpBufferStr(&buf,
            "\n     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid"
            "\n     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid");

    if (showNested || pattern)
        appendPQExpBufferStr(&buf,
            "\n     LEFT JOIN pg_catalog.pg_inherits inh ON c.oid = inh.inhrelid");

    // Add size calculation subquery for verbose mode
    if (verbose) {
        if (pset.sversion < 120000) {
            // Use recursive CTE for pre-12.0
            appendPQExpBufferStr(&buf,
                ",\n     LATERAL (WITH RECURSIVE d\n"
                "                AS (SELECT inhrelid AS oid, 1 AS level\n"
                "                      FROM pg_catalog.pg_inherits\n"
                "                     WHERE inhparent = c.oid\n"
                "                    UNION ALL\n"
                "                    SELECT inhrelid, level + 1\n"
                "                      FROM pg_catalog.pg_inherits i\n"
                "                           JOIN d ON i.inhparent = d.oid)\n"
                "                SELECT pg_catalog.pg_size_pretty(sum(pg_catalog.pg_table_size("
                "d.oid))) AS tps,\n"
                "                       pg_catalog.pg_size_pretty(sum(\n"
                "             CASE WHEN d.level = 1"
                " THEN pg_catalog.pg_table_size(d.oid) ELSE 0 END)) AS dps\n"
                "               FROM d) s");
        } else {
            // Use pg_partition_tree for 12.0+
            appendPQExpBufferStr(&buf,
                ",\n     LATERAL (SELECT pg_catalog.pg_size_pretty(sum(\n"
                "                 CASE WHEN ppt.isleaf AND ppt.level = 1\n"
                "                      THEN pg_catalog.pg_table_size(ppt.relid)"
                " ELSE 0 END)) AS dps"
                ",\n                     pg_catalog.pg_size_pretty(sum("
                "pg_catalog.pg_table_size(ppt.relid))) AS tps"
                "\n              FROM pg_catalog.pg_partition_tree(c.oid) ppt) s");
        }
    }

    // Build WHERE clause
    appendPQExpBufferStr(&buf, "\nWHERE c.relkind IN (");
    if (showTables) appendPQExpBufferStr(&buf, "'p',");  // partitioned table
    if (showIndexes) appendPQExpBufferStr(&buf, "'I',"); // partitioned index
    appendPQExpBufferStr(&buf, "'')");  // dummy
    appendPQExpBufferStr(&buf, ")\n");

    // Filter out leaf partitions unless nested view or pattern
    appendPQExpBufferStr(&buf, !showNested && !pattern ?
                         " AND NOT c.relispartition\n" : "");

    // Exclude system schemas unless pattern provided
    if (!pattern)
        appendPQExpBufferStr(&buf,
            "      AND n.nspname <> 'pg_catalog'\n"
            "      AND n.nspname !~ '^pg_toast'\n"
            "      AND n.nspname <> 'information_schema'\n");

    // Add pattern validation
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                                "n.nspname", "c.relname", NULL,
                                "pg_catalog.pg_table_is_visible(c.oid)",
                                NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    // Add ORDER BY clause
    appendPQExpBuffer(&buf, "ORDER BY \"Schema\", %s%s\"Name\";",
                      mixed_output ? "\"Type\" DESC, " : "",
                      showNested || pattern ? "\"Parent name\" NULLS FIRST, " : "");

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    // Display results
    initPQExpBuffer(&title);
    appendPQExpBufferStr(&title, tabletitle);

    myopt.title = title.data;
    myopt.translate_header = true;
    myopt.translate_columns = translate_columns;
    myopt.n_translate_columns = lengthof(translate_columns);

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    termPQExpBuffer(&title);
    PQclear(res);
    return true;
}
```