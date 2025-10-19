# listOperatorFamilies

## Location
[src/bin/psql/describe.c:6778-6866](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6778-L6866)

## Overview
The  function implements the  psql command to display a formatted list of operator families, with optional filtering by index access method and applicable data types.

## Definition

```c
bool
listOperatorFamilies(const char *access_method_pattern,
					 const char *type_pattern, bool verbose)
```
## Detailed Description
This function constructs and executes an SQL query to retrieve operator family information from PostgreSQL system catalogs. It displays operator families with their associated access methods, family names, and a comma-separated list of applicable types (derived from operator classes that belong to the family). In verbose mode, it additionally shows the owner information. The function supports pattern matching for filtering results by access method name and by types that are applicable to the family.

The query joins multiple system catalogs (, , ) and uses a correlated subquery to aggregate applicable types from . When filtering by type pattern, it uses an EXISTS subquery to check if any operator class in the family matches the specified type pattern. Results are sorted by access method and operator family name for consistent presentation.

## Parameters / Member Variables
- `*access_method_pattern`: Optional regex pattern to filter results by index access method name (e.g., "btree", "hash"). If NULL, all access methods are included.
- `*type_pattern`: Optional regex pattern to filter results by applicable data types. Matches against both internal type names and external formatted type names within operator classes that belong to the family. If NULL, all families are included regardless of their applicable types.
- `verbose`: Boolean flag that controls whether to include additional columns (owner) in the output.
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - lengthof
  - [printQuery](../p/printQuery.md)
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - [printQueryOpt](../p/printQueryOpt.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher)

## Notes and Other Information
- This function is part of psql's describe command family, specifically handling the  command
- Pattern matching follows PostgreSQL's standard SQL pattern syntax with support for wildcards
- The function uses internationalization support through gettext_noop() for column headers
- Error handling includes proper cleanup of allocated buffers on failure paths
- The query uses visibility functions like  to handle schema-qualified names appropriately
- Applicable types are aggregated using  to show all types supported by operator classes within the family
- When filtering by type pattern, the function uses an EXISTS clause with a join to  to find families that contain operator classes for matching types

## Simplified Source

```c
bool listOperatorFamilies(const char *access_method_pattern,
                         const char *type_pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    bool have_where = false;

    initPQExpBuffer(&buf);

    // Build main query with operator family information
    printfPQExpBuffer(&buf,
        "SELECT "
        "am.amname AS \"AM\", "
        "CASE "
        "  WHEN pg_catalog.pg_opfamily_is_visible(f.oid) "
        "  THEN pg_catalog.format('%%I', f.opfname) "
        "  ELSE pg_catalog.format('%%I.%%I', n.nspname, f.opfname) "
        "END AS \"Operator family\", "
        "(SELECT "
        "  pg_catalog.string_agg(pg_catalog.format_type(oc.opcintype, NULL), ', ') "
        " FROM pg_catalog.pg_opclass oc "
        " WHERE oc.opcfamily = f.oid) \"Applicable types\"");

    // Add verbose columns if requested
    if (verbose) {
        appendPQExpBuffer(&buf,
            ", pg_catalog.pg_get_userbyid(f.opfowner) AS \"Owner\"");
    }

    // Add FROM clause with joins
    appendPQExpBufferStr(&buf,
        " FROM pg_catalog.pg_opfamily f "
        "LEFT JOIN pg_catalog.pg_am am ON am.oid = f.opfmethod "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = f.opfnamespace");

    // Apply access method pattern filter
    if (access_method_pattern) {
        if (!validateSQLNamePattern(&buf, access_method_pattern,
                                   false, false, NULL, "am.amname",
                                   NULL, NULL, &have_where, 1))
            goto error_return;
    }

    // Apply type pattern filter using EXISTS subquery
    if (type_pattern) {
        appendPQExpBuffer(&buf,
            " %s EXISTS ("
            "  SELECT 1 "
            "  FROM pg_catalog.pg_type t "
            "  JOIN pg_catalog.pg_opclass oc ON oc.opcintype = t.oid "
            "  LEFT JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace "
            "  WHERE oc.opcfamily = f.oid",
            have_where ? "AND" : "WHERE");

        if (!validateSQLNamePattern(&buf, type_pattern, true, false,
                                   "tn.nspname", "t.typname",
                                   "pg_catalog.format_type(t.oid, NULL)",
                                   "pg_catalog.pg_type_is_visible(t.oid)",
                                   NULL, 3))
            goto error_return;

        appendPQExpBufferStr(&buf, " )");
    }

    appendPQExpBufferStr(&buf, " ORDER BY 1, 2;");

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    // Set up output formatting and display results
    myopt.title = "List of operator families";
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```