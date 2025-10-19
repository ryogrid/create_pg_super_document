# describeFunctions

## Location
[src/bin/psql/describe.c:288-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L288-L614)

## Overview
Implements the \df psql command to display a comprehensive list of functions in the database, supporting multiple function types, pattern matching, argument filtering, and extensive verbose information.

## Definition
```c
bool describeFunctions(const char *functypes, const char *func_pattern,
                      char **arg_patterns, int num_arg_patterns,
                      bool verbose, bool showSystem)
```

## Detailed Description
This is the most complex describe function in psql, handling the \df command with sophisticated filtering capabilities. It constructs dynamic SQL queries to list functions from pg_proc and related catalogs, supporting function type filtering (aggregate, normal, procedure, trigger, window), pattern matching on function names, argument type pattern matching, and comprehensive verbose output. The function handles PostgreSQL version differences (particularly for procedures introduced in v11 and parallel safety in v9.6) and provides extensive internationalization support.

## Parameters / Member Variables
- `functypes`: String containing function type specifiers ('a'=aggregate, 'n'=normal, 'p'=procedure, 't'=trigger, 'w'=window)
- `func_pattern`: Optional regular expression pattern to filter functions by name or schema
- `arg_patterns`: Array of patterns to match against function argument types
- `num_arg_patterns`: Number of argument patterns provided
- `verbose`: Boolean flag to include extensive additional information (volatility, parallel safety, owner, security, ACL, language, etc.)
- `showSystem`: Boolean flag to control whether system schema functions are displayed

## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [map_typename_pattern](../m/map_typename_pattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - lengthof
- Called from (representative examples):
  - [exec_command_dfo](../e/exec_command_dfo.md) (in command.c:1062)

## Notes and Other Information
- Part of psql's describe functionality (\df command)
- Supports complex function type filtering with multiple simultaneous options
- Handles PostgreSQL version compatibility for procedures (v11+) and parallel safety (v9.6+)
- Uses dynamic JOIN construction for argument type pattern matching
- Provides comprehensive verbose output including volatility, parallel safety, ownership, security context, ACL, and language information
- Supports special '-' pattern in argument matching to specify 'no parameter at this position'
- Uses different column translation arrays for different PostgreSQL versions
- Includes sophisticated WHERE clause construction based on function type selections
- Validates function type string against allowed characters (anptwS+)
- Returns early with helpful error messages for unsupported options on older servers

## Simplified Source

```c
bool
describeFunctions(const char *functypes, const char *func_pattern,
                  char **arg_patterns, int num_arg_patterns,
                  bool verbose, bool showSystem)
{
    // Parse function type flags
    bool showAggregate = strchr(functypes, 'a') != NULL;
    bool showNormal = strchr(functypes, 'n') != NULL;
    bool showProcedure = strchr(functypes, 'p') != NULL;
    bool showTrigger = strchr(functypes, 't') != NULL;
    bool showWindow = strchr(functypes, 'w') != NULL;

    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Validate function types string
    if (strlen(functypes) != strspn(functypes, "anptwS+")) {
        pg_log_error("\\df only takes [anptwS+] as options");
        return true;
    }

    // Check version compatibility for procedures
    if (showProcedure && pset.sversion < 110000) {
        // Show error and return
        return true;
    }

    // Default to all function types if none specified
    if (!showAggregate && !showNormal && !showProcedure && !showTrigger && !showWindow) {
        showAggregate = showNormal = showTrigger = showWindow = true;
        if (pset.sversion >= 110000)
            showProcedure = true;
    }

    initPQExpBuffer(&buf);

    // Build base query with schema and function name
    printfPQExpBuffer(&buf,
                      "SELECT n.nspname as \"%s\",\n"
                      "  p.proname as \"%s\",\n",
                      gettext_noop("Schema"),
                      gettext_noop("Name"));

    // Add result type, arguments, and function type classification (version-dependent)
    if (pset.sversion >= 110000) {
        // Use prokind column for newer PostgreSQL versions
        appendPQExpBuffer(&buf,
                          "  pg_catalog.pg_get_function_result(p.oid) as \"%s\",\n"
                          "  pg_catalog.pg_get_function_arguments(p.oid) as \"%s\",\n"
                          " CASE p.prokind\n"
                          "  WHEN 'a' THEN '%s'\n"
                          "  WHEN 'w' THEN '%s'\n"
                          "  WHEN 'p' THEN '%s'\n"
                          "  ELSE '%s'\n"
                          " END as \"%s\"",
                          gettext_noop("Result data type"),
                          gettext_noop("Argument data types"),
                          gettext_noop("agg"), gettext_noop("window"),
                          gettext_noop("proc"), gettext_noop("func"),
                          gettext_noop("Type"));
    } else {
        // Use legacy boolean columns for older versions
        appendPQExpBuffer(&buf,
                          "  pg_catalog.pg_get_function_result(p.oid) as \"%s\",\n"
                          "  pg_catalog.pg_get_function_arguments(p.oid) as \"%s\",\n"
                          " CASE\n"
                          "  WHEN p.proisagg THEN '%s'\n"
                          "  WHEN p.proiswindow THEN '%s'\n"
                          "  WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN '%s'\n"
                          "  ELSE '%s'\n"
                          " END as \"%s\"",
                          gettext_noop("Result data type"),
                          gettext_noop("Argument data types"),
                          gettext_noop("agg"), gettext_noop("window"),
                          gettext_noop("trigger"), gettext_noop("func"),
                          gettext_noop("Type"));
    }

    // Add verbose columns if requested (volatility, parallel, owner, security, ACL, language, etc.)
    if (verbose) {
        // Add volatility, parallel safety, owner, security, ACL, language, description
        // (simplified representation of complex verbose logic)
        appendPQExpBuffer(&buf, ",\n [verbose columns...]");
    }

    // FROM clause with necessary joins
    appendPQExpBufferStr(&buf,
                         "\nFROM pg_catalog.pg_proc p"
                         "\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n");

    // Add argument type joins for pattern matching
    for (int i = 0; i < num_arg_patterns; i++) {
        appendPQExpBuffer(&buf,
                          "     LEFT JOIN pg_catalog.pg_type t%d ON t%d.oid = p.proargtypes[%d]\n"
                          "     LEFT JOIN pg_catalog.pg_namespace nt%d ON nt%d.oid = t%d.typnamespace\n",
                          i, i, i, i, i, i);
    }

    // Build WHERE clause based on function type selections
    bool have_where = false;
    // (Complex logic for filtering by function types - simplified)

    // Apply pattern filtering and argument patterns
    if (!validateSQLNamePattern(&buf, func_pattern, have_where, false,
                                "n.nspname", "p.proname", NULL,
                                "pg_catalog.pg_function_is_visible(p.oid)",
                                NULL, 3))
        goto error_return;

    // Process argument patterns
    for (int i = 0; i < num_arg_patterns; i++) {
        if (strcmp(arg_patterns[i], "-") != 0) {
            // Apply argument type pattern matching
        }
    }

    // Filter system schemas if not requested
    if (!showSystem && !func_pattern)
        appendPQExpBufferStr(&buf, "      AND n.nspname <> 'pg_catalog'\n"
                                   "      AND n.nspname <> 'information_schema'\n");

    appendPQExpBufferStr(&buf, "ORDER BY 1, 2, 4;");

    // Execute and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of functions");
    myopt.translate_header = true;
    // Set appropriate translation columns based on version

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);
    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```