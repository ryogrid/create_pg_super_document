# describeOperators

## Location
[src/bin/psql/describe.c:770-910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L770-L910)

## Overview
Implements the \do psql command to describe PostgreSQL operators, displaying their names, argument types, result types, and optionally their underlying functions and descriptions.

## Definition

```c
bool
describeOperators(const char *oper_pattern,
				  char **arg_patterns, int num_arg_patterns,
				  bool verbose, bool showSystem)
```
## Detailed Description
This function generates and executes a complex SQL query to retrieve operator information from PostgreSQL system catalogs. It constructs a detailed view of operators including their schema, name, left/right argument types, result type, and descriptions. The function supports pattern matching for both operator names and argument types, with special handling for prefix operators (when only one argument pattern is provided).

The function builds a SQL query that joins pg_operator with pg_namespace and optionally with pg_type tables for argument type filtering. It includes backward compatibility support for postfix operators (dead code as of PostgreSQL 14 but maintained for older server versions) and provides fallback comment lookup from the operator's underlying function for operators without direct comments.

The query results are formatted and displayed using psql's standard table printing mechanisms with appropriate column headers and internationalization support.

## Parameters / Member Variables
- `*oper_pattern`: Pattern to match operator names (can be NULL for all operators)
- `**arg_patterns`: Array of patterns to match argument types (can contain "-" for no argument)
- `num_arg_patterns`: Number of argument patterns provided (0-2, additional patterns ignored)
- `verbose`: If true, includes the underlying function name in output
- `showSystem`: If true, includes system schema operators (pg_catalog, information_schema)
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize query buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (format SQL query)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and add pattern matching clauses)
  - [map_typename_pattern](../m/map_typename_pattern.md) (normalize type name patterns)
  - [PSQLexec](../P/PSQLexec.md) (execute SQL query)
  - [printQuery](../p/printQuery.md) (display results)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup query buffer)
- Called from (representative examples):
  - [exec_command_dfo](../e/exec_command_dfo.md) (src/bin/psql/command.c:1066) - handles \do command
  - Declared in DESCRIBE_H (src/bin/psql/describe.h:30)

## Notes and Other Information
- Supports complex operator filtering by both name and argument type patterns
- Handles unary prefix operators when num_arg_patterns == 1 by adding "o.oprleft = 0" constraint
- Uses coalesce() to provide fallback comment lookup from operator's function (legacy support)
- Maintains compatibility with pre-PostgreSQL 14 servers by including postfix operator support
- Argument patterns of "-" specifically indicate no argument should exist for that position
- Results are ordered by schema, name, left arg type, and right arg type for consistent display
- Returns boolean success/failure status like other psql describe functions
- The verbose flag adds the "Function" column showing the underlying implementation function

## Simplified Source

```c
bool
describeOperators(const char *oper_pattern,
                  char **arg_patterns, int num_arg_patterns,
                  bool verbose, bool showSystem)
{
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    initPQExpBuffer(&buf);

    // Build base query for operators
    printfPQExpBuffer(&buf,
                      "SELECT n.nspname as \"%s\",\n"
                      "  o.oprname AS \"%s\",\n"
                      "  CASE WHEN o.oprkind='l' THEN NULL ELSE pg_catalog.format_type(o.oprleft, NULL) END AS \"%s\",\n"
                      "  CASE WHEN o.oprkind='r' THEN NULL ELSE pg_catalog.format_type(o.oprright, NULL) END AS \"%s\",\n"
                      "  pg_catalog.format_type(o.oprresult, NULL) AS \"%s\",\n",
                      gettext_noop("Schema"),
                      gettext_noop("Name"),
                      gettext_noop("Left arg type"),
                      gettext_noop("Right arg type"),
                      gettext_noop("Result type"));

    // Add function column if verbose
    if (verbose)
        appendPQExpBuffer(&buf,
                          "  o.oprcode AS \"%s\",\n",
                          gettext_noop("Function"));

    // Add description with fallback to function comment
    appendPQExpBuffer(&buf,
                      "  coalesce(pg_catalog.obj_description(o.oid, 'pg_operator'),\n"
                      "           pg_catalog.obj_description(o.oprcode, 'pg_proc')) AS \"%s\"\n"
                      "FROM pg_catalog.pg_operator o\n"
                      "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace\n",
                      gettext_noop("Description"));

    // Add argument type joins for pattern matching
    if (num_arg_patterns >= 2) {
        num_arg_patterns = 2;  // Limit to 2 arguments
        appendPQExpBufferStr(&buf,
                             "     LEFT JOIN pg_catalog.pg_type t0 ON t0.oid = o.oprleft\n"
                             "     LEFT JOIN pg_catalog.pg_namespace nt0 ON nt0.oid = t0.typnamespace\n"
                             "     LEFT JOIN pg_catalog.pg_type t1 ON t1.oid = o.oprright\n"
                             "     LEFT JOIN pg_catalog.pg_namespace nt1 ON nt1.oid = t1.typnamespace\n");
    } else if (num_arg_patterns == 1) {
        appendPQExpBufferStr(&buf,
                             "     LEFT JOIN pg_catalog.pg_type t0 ON t0.oid = o.oprright\n"
                             "     LEFT JOIN pg_catalog.pg_namespace nt0 ON nt0.oid = t0.typnamespace\n");
    }

    // Filter system schemas if not requested
    if (!showSystem && !oper_pattern)
        appendPQExpBufferStr(&buf, "WHERE n.nspname <> 'pg_catalog'\n"
                                   "      AND n.nspname <> 'information_schema'\n");

    // Apply operator pattern filtering
    if (!validateSQLNamePattern(&buf, oper_pattern,
                                !showSystem && !oper_pattern, true,
                                "n.nspname", "o.oprname", NULL,
                                "pg_catalog.pg_operator_is_visible(o.oid)",
                                NULL, 3))
        goto error_return;

    // For single argument pattern, ensure it's a prefix operator
    if (num_arg_patterns == 1)
        appendPQExpBufferStr(&buf, "  AND o.oprleft = 0\n");

    // Process argument type patterns
    for (int i = 0; i < num_arg_patterns; i++) {
        if (strcmp(arg_patterns[i], "-") != 0) {
            // Apply argument type pattern matching
            char nspname[64], typname[64], ft[64], tiv[64];

            snprintf(nspname, sizeof(nspname), "nt%d.nspname", i);
            snprintf(typname, sizeof(typname), "t%d.typname", i);
            snprintf(ft, sizeof(ft), "pg_catalog.format_type(t%d.oid, NULL)", i);
            snprintf(tiv, sizeof(tiv), "pg_catalog.pg_type_is_visible(t%d.oid)", i);

            if (!validateSQLNamePattern(&buf,
                                        map_typename_pattern(arg_patterns[i]),
                                        true, false,
                                        nspname, typname, ft, tiv,
                                        NULL, 3))
                goto error_return;
        } else {
            // "-" pattern specifies no such parameter
            appendPQExpBuffer(&buf, "  AND t%d.typname IS NULL\n", i);
        }
    }

    appendPQExpBufferStr(&buf, "ORDER BY 1, 2, 3, 4;");

    // Execute query and display results
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    myopt.title = _("List of operators");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);
    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```