# describeTypes

## Location
[src/bin/psql/describe.c:615-719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L615-L719)

## Overview
Implements the \dT psql command to display a comprehensive list of data types in the database, with sophisticated filtering to exclude unwanted complex types and array types.

## Definition
```c
bool describeTypes(const char *pattern, bool verbose, bool showSystem)
```

## Detailed Description
This function generates and executes a SQL query to list data types from the pg_type system catalog. It implements intelligent filtering logic to exclude complex types (unless they are standalone composite types) and array types (unless explicitly requested with '[]' in the pattern). The function constructs queries that show both internal and formatted type names, with verbose mode providing additional details including internal names, sizes, enum elements, ownership, ACL information, and descriptions. It supports pattern matching against both internal type names and formatted type displays.

## Parameters / Member Variables
- `pattern`: Optional regular expression pattern to filter types by name (supports '[]' to include array types)
- `verbose`: Boolean flag to include additional columns (internal name, size, elements, owner, ACL)
- `showSystem`: Boolean flag to control whether system schema types are displayed

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [map_typename_pattern](../m/map_typename_pattern.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - CppAsString2
  - RELKIND_COMPOSITE_TYPE
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c:923)

## Notes and Other Information
- Part of psql's describe functionality (\dT command)
- Implements sophisticated type filtering logic to provide clean, useful type listings
- Excludes complex types (typrelid!=0) unless they are standalone composite types
- Excludes array types by default unless pattern contains '[]' 
- In verbose mode, shows enum elements with proper sorting (enumsortorder)
- Uses format_type() for user-friendly type name display
- Matches patterns against both internal names (typname) and formatted names
- Shows type size information with special handling for variable-length ('var') and tuple types
- Provides access control information and ownership details in verbose mode
- Uses map_typename_pattern() for enhanced pattern matching capabilities
- Orders results by schema and type name for consistent output

## Simplified Source

```c
bool
describeTypes(const char *pattern, bool verbose, bool showSystem)
{
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    initPQExpBuffer(&buf);

    // Build base query for types
    printfPQExpBuffer(&buf,
                      "SELECT n.nspname as \"%s\",\n"
                      "  pg_catalog.format_type(t.oid, NULL) AS \"%s\",\n",
                      gettext_noop("Schema"),
                      gettext_noop("Name"));

    // Add verbose columns if requested
    if (verbose) {
        appendPQExpBuffer(&buf,
                          "  t.typname AS \"%s\",\n"
                          "  CASE WHEN t.typrelid != 0\n"
                          "      THEN CAST('tuple' AS pg_catalog.text)\n"
                          "    WHEN t.typlen < 0\n"
                          "      THEN CAST('var' AS pg_catalog.text)\n"
                          "    ELSE CAST(t.typlen AS pg_catalog.text)\n"
                          "  END AS \"%s\",\n"
                          "  pg_catalog.array_to_string(\n"
                          "      ARRAY(\n"
                          "          SELECT e.enumlabel\n"
                          "          FROM pg_catalog.pg_enum e\n"
                          "          WHERE e.enumtypid = t.oid\n"
                          "          ORDER BY e.enumsortorder\n"
                          "      ),\n"
                          "      E'\\n'\n"
                          "  ) AS \"%s\",\n"
                          "  pg_catalog.pg_get_userbyid(t.typowner) AS \"%s\",\n",
                          gettext_noop("Internal name"),
                          gettext_noop("Size"),
                          gettext_noop("Elements"),
                          gettext_noop("Owner"));
        printACLColumn(&buf, "t.typacl");
        appendPQExpBufferStr(&buf, ",\n  ");
    }

    appendPQExpBuffer(&buf,
                      "  pg_catalog.obj_description(t.oid, 'pg_type') as \"%s\"\n",
                      gettext_noop("Description"));

    appendPQExpBufferStr(&buf, "FROM pg_catalog.pg_type t\n"
                               "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n");

    // Filter complex types (keep only standalone composite types)
    appendPQExpBufferStr(&buf, "WHERE (t.typrelid = 0 ");
    appendPQExpBufferStr(&buf, "OR (SELECT c.relkind = " CppAsString2(RELKIND_COMPOSITE_TYPE)
                               " FROM pg_catalog.pg_class c "
                               "WHERE c.oid = t.typrelid))\n");

    // Filter array types unless pattern contains []
    if (pattern == NULL || strstr(pattern, "[]") == NULL)
        appendPQExpBufferStr(&buf, "  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)\n");

    // Filter system schemas if not requested
    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf, "      AND n.nspname <> 'pg_catalog'\n"
                                   "      AND n.nspname <> 'information_schema'\n");

    // Apply pattern matching (supports both internal and external names)
    if (!validateSQLNamePattern(&buf, map_typename_pattern(pattern),
                                true, false,
                                "n.nspname", "t.typname",
                                "pg_catalog.format_type(t.oid, NULL)",
                                "pg_catalog.pg_type_is_visible(t.oid)",
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

    myopt.title = _("List of data types");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);
    return true;
}
```