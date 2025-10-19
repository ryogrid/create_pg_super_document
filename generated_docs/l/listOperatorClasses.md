# listOperatorClasses

## Location
[src/bin/psql/describe.c:6677-6777](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6677-L6777)

## Overview
The  function implements the  psql command to display a formatted list of operator classes, with optional filtering by index access method and input data type.

## Definition

```c
bool
listOperatorClasses(const char *access_method_pattern,
					const char *type_pattern, bool verbose)
```
## Detailed Description
This function constructs and executes an SQL query to retrieve operator class information from PostgreSQL system catalogs. It displays operator classes with their associated access methods, input types, storage types (when different from input type), names, and default status. In verbose mode, it additionally shows the operator family and owner information. The function supports pattern matching for filtering results by access method name and type name, using PostgreSQL's standard pattern matching syntax.

The query joins multiple system catalogs (, , , , and optionally ) to gather comprehensive information about operator classes. Results are sorted by access method, input type, and operator class name for consistent presentation.

## Parameters / Member Variables
- `*access_method_pattern`: Optional regex pattern to filter results by index access method name (e.g., "btree", "hash"). If NULL, all access methods are included.
- `*type_pattern`: Optional regex pattern to filter results by input data type name. Matches against both internal type names and external formatted type names. If NULL, all types are included.
- `verbose`: Boolean flag that controls whether to include additional columns (operator family and owner) in the output.
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
- Default operator classes are identified by the  boolean field and displayed as "yes"/"no"

## Simplified Source

```c
bool listOperatorClasses(const char *access_method_pattern,
                        const char *type_pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    bool have_where = false;

    initPQExpBuffer(&buf);

    // Build main query with core operator class information
    printfPQExpBuffer(&buf,
        "SELECT "
        "am.amname AS \"AM\", "
        "pg_catalog.format_type(c.opcintype, NULL) AS \"Input type\", "
        "CASE "
        "  WHEN c.opckeytype <> 0 AND c.opckeytype <> c.opcintype "
        "  THEN pg_catalog.format_type(c.opckeytype, NULL) "
        "  ELSE NULL "
        "END AS \"Storage type\", "
        "CASE "
        "  WHEN pg_catalog.pg_opclass_is_visible(c.oid) "
        "  THEN pg_catalog.format('%%I', c.opcname) "
        "  ELSE pg_catalog.format('%%I.%%I', n.nspname, c.opcname) "
        "END AS \"Operator class\", "
        "(CASE WHEN c.opcdefault "
        "  THEN 'yes' "
        "  ELSE 'no' "
        "END) AS \"Default?\"");

    // Add verbose columns if requested
    if (verbose) {
        appendPQExpBuffer(&buf,
            ", CASE "
            "  WHEN pg_catalog.pg_opfamily_is_visible(of.oid) "
            "  THEN pg_catalog.format('%%I', of.opfname) "
            "  ELSE pg_catalog.format('%%I.%%I', ofn.nspname, of.opfname) "
            "END AS \"Operator family\", "
            "pg_catalog.pg_get_userbyid(c.opcowner) AS \"Owner\"");
    }

    // Add FROM clause with joins
    appendPQExpBufferStr(&buf,
        " FROM pg_catalog.pg_opclass c "
        "LEFT JOIN pg_catalog.pg_am am ON am.oid = c.opcmethod "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.opcnamespace "
        "LEFT JOIN pg_catalog.pg_type t ON t.oid = c.opcintype "
        "LEFT JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace");

    if (verbose) {
        appendPQExpBufferStr(&buf,
            " LEFT JOIN pg_catalog.pg_opfamily of ON of.oid = c.opcfamily "
            "LEFT JOIN pg_catalog.pg_namespace ofn ON ofn.oid = of.opfnamespace");
    }

    // Apply access method pattern filter
    if (access_method_pattern) {
        if (!validateSQLNamePattern(&buf, access_method_pattern,
                                   false, false, NULL, "am.amname",
                                   NULL, NULL, &have_where, 1))
            goto error_return;
    }

    // Apply type pattern filter
    if (type_pattern) {
        if (!validateSQLNamePattern(&buf, type_pattern, have_where, false,
                                   "tn.nspname", "t.typname",
                                   "pg_catalog.format_type(t.oid, NULL)",
                                   "pg_catalog.pg_type_is_visible(t.oid)",
                                   NULL, 3))
            goto error_return;
    }

    appendPQExpBufferStr(&buf, " ORDER BY 1, 2, 4;");

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    // Set up output formatting and display results
    myopt.title = "List of operator classes";
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```