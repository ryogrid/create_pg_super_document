# permissionsList

## Location
[src/bin/psql/describe.c:1011-1174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L1011-L1174)

## Overview
Implements the \z and \dp psql commands to display access privileges (grants and revokes) for database tables, views, materialized views, sequences, foreign tables, and partitioned tables.

## Definition

```c
bool
permissionsList(const char *pattern, bool showSystem)
```
## Detailed Description
This function constructs and executes a comprehensive SQL query to retrieve access control information from PostgreSQL system catalogs. It displays detailed privilege information including table-level permissions, column-level privileges, and row-level security policies. The output includes schema name, object name, object type, access privileges, column privileges, and policies (when supported by the server version).

The function adapts its query based on PostgreSQL server version to handle the evolution of row-level security features. For servers version 9.5-9.6, it displays basic policy information. For PostgreSQL 10+, it includes support for RESTRICTIVE policies, showing whether policies are permissive or restrictive in nature.

The query excludes indexes and toast tables as they have no meaningful access rights. It formats complex privilege information in a human-readable way, including policy details with USING and WITH CHECK expressions, and role-based policy assignments.

## Parameters / Member Variables
- `*pattern`: SQL pattern to filter object names (can be NULL to show all objects)
- `showSystem`: If true, includes system schema objects (pg_catalog, information_schema)
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize query buffer)
  - [printfPQExpBuffer](printfPQExpBuffer.md) (format base SQL query)
  - [printACLColumn](printACLColumn.md) (format access control list for table-level privileges)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and apply name pattern filtering)
  - [PSQLexec](../P/PSQLexec.md) (execute the constructed SQL query)
  - [printQuery](printQuery.md) (display formatted results with translation support)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup query buffer)
  - RELKIND constants (RELKIND_RELATION, RELKIND_VIEW, RELKIND_MATVIEW, etc.)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (src/bin/psql/command.c:903) - handles \dp command variant
  - [exec_command_z](../e/exec_command_z.md) (src/bin/psql/command.c:3036) - handles \z command
  - Declared in DESCRIBE_H (src/bin/psql/describe.h:44)

## Notes and Other Information
- Excludes indexes and toast tables from results as they have no meaningful access rights
- Supports version-specific features: basic policies (9.5+) and restrictive policies (10+)
- Column privileges are displayed in a nested format showing column name followed by its ACL
- Policy information includes command type, USING expressions (u), WITH CHECK expressions (c), and applicable roles
- For PostgreSQL 10+, distinguishes between PERMISSIVE and RESTRICTIVE policies
- Uses translate_columns array to control which columns should be translated for internationalization
- Results are ordered by schema name and object name for consistent presentation
- Object types are translated for display (table, view, materialized view, sequence, foreign table, partitioned table)
- Returns boolean status indicating success/failure of the operation

## Simplified Source

```c
bool permissionsList(const char *pattern, bool showSystem) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    static const bool translate_columns[] = {false, false, true, false, false, false};

    initPQExpBuffer(&buf);

    // Build query to show schema, name, type, and permissions for database objects
    printfPQExpBuffer(&buf,
        "SELECT n.nspname as \"Schema\",\n"
        "  c.relname as \"Name\",\n"
        "  CASE c.relkind"
        " WHEN 'r' THEN 'table'"
        " WHEN 'v' THEN 'view'"
        " WHEN 'm' THEN 'materialized view'"
        " WHEN 'S' THEN 'sequence'"
        " WHEN 'f' THEN 'foreign table'"
        " WHEN 'p' THEN 'partitioned table'"
        " END as \"Type\",\n"
        "  ");

    // Add table-level access privileges column
    printACLColumn(&buf, "c.relacl");

    // Add column privileges information
    appendPQExpBuffer(&buf,
        ",\n  pg_catalog.array_to_string(ARRAY(\n"
        "    SELECT attname || ':\\n  ' || pg_catalog.array_to_string(attacl, '\\n  ')\n"
        "    FROM pg_catalog.pg_attribute a\n"
        "    WHERE attrelid = c.oid AND NOT attisdropped AND attacl IS NOT NULL\n"
        "  ), '\\n') AS \"Column privileges\"");

    // Add policy information for supported versions
    if (pset.sversion >= 90500 && pset.sversion < 100000) {
        // Basic policy support for 9.5-9.6
        appendPQExpBuffer(&buf,
            ",\n  pg_catalog.array_to_string(ARRAY(\n"
            "    SELECT polname || ' (' || polcmd || ')'\n"
            "    || CASE WHEN polqual IS NOT NULL THEN\n"
            "           '\\n  (u): ' || pg_catalog.pg_get_expr(polqual, polrelid)\n"
            "       ELSE ''\n"
            "       END\n"
            "    || CASE WHEN polwithcheck IS NOT NULL THEN\n"
            "           '\\n  (c): ' || pg_catalog.pg_get_expr(polwithcheck, polrelid)\n"
            "       ELSE ''\n"
            "       END\n"
            "    FROM pg_catalog.pg_policy pol\n"
            "    WHERE polrelid = c.oid), '\\n')\n"
            "    AS \"Policies\"");
    }

    if (pset.sversion >= 100000) {
        // Enhanced policy support for 10+ with RESTRICTIVE policies
        appendPQExpBuffer(&buf,
            ",\n  pg_catalog.array_to_string(ARRAY(\n"
            "    SELECT polname\n"
            "    || CASE WHEN NOT polpermissive THEN ' (RESTRICTIVE)' ELSE '' END\n"
            "    || ' (' || polcmd || ')'\n"
            "    || CASE WHEN polqual IS NOT NULL THEN\n"
            "           '\\n  (u): ' || pg_catalog.pg_get_expr(polqual, polrelid)\n"
            "       ELSE ''\n"
            "       END\n"
            "    || CASE WHEN polwithcheck IS NOT NULL THEN\n"
            "           '\\n  (c): ' || pg_catalog.pg_get_expr(polwithcheck, polrelid)\n"
            "       ELSE ''\n"
            "       END\n"
            "    FROM pg_catalog.pg_policy pol\n"
            "    WHERE polrelid = c.oid), '\\n')\n"
            "    AS \"Policies\"");
    }

    // Add FROM clause - only include meaningful object types (no indexes/toast)
    appendPQExpBufferStr(&buf,
        "\nFROM pg_catalog.pg_class c\n"
        "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
        "WHERE c.relkind IN ('r','v','m','S','f','p')\n");

    // Filter out system schemas unless requested
    if (!showSystem && !pattern) {
        appendPQExpBufferStr(&buf,
            "      AND n.nspname <> 'pg_catalog'\n"
            "      AND n.nspname <> 'information_schema'\n");
    }

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                                "n.nspname", "c.relname", NULL,
                                "pg_catalog.pg_table_is_visible(c.oid)",
                                NULL, 3))
        goto error_return;

    // Execute query and display results
    appendPQExpBufferStr(&buf, "ORDER BY 1, 2;");
    res = PSQLexec(buf.data);
    if (!res)
        goto error_return;

    // Set display options
    printfPQExpBuffer(&buf, "Access privileges");
    myopt.title = buf.data;
    myopt.translate_header = true;
    myopt.translate_columns = translate_columns;
    myopt.n_translate_columns = lengthof(translate_columns);

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    termPQExpBuffer(&buf);
    PQclear(res);
    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```