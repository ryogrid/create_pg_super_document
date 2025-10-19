# listDomains

## Location
[src/bin/psql/describe.c:4383-4465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4383-L4465)

## Overview
Lists and displays information about user-defined domains in the PostgreSQL database, corresponding to the psql \dD command.

## Definition
bool listDomains(const char *pattern, bool verbose, bool showSystem)

## Detailed Description
The listDomains function generates and executes a SQL query to retrieve information about domains from the pg_catalog.pg_type system catalog. It displays domain details including schema, name, underlying base type, collation, nullable constraints, default values, and check constraints. When verbose mode is enabled, it includes access control lists (ACLs) and descriptions. The function specifically filters for domain types (typtype = 'd') and can optionally exclude system schemas based on the showSystem parameter. The query uses complex subqueries to extract collation information and aggregate check constraints into a readable format.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter domain names by schema and/or name (can be NULL for no filtering)
- `verbose`: Boolean flag to include extended information (ACLs, descriptions) in the output
- `showSystem`: Boolean flag to include domains from system schemas (pg_catalog, information_schema)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)  
  - [printACLColumn](../p/printACLColumn.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher)

## Notes and Other Information
- Implements the \dD psql meta-command functionality
- Queries pg_catalog.pg_type with typtype = 'd' to find domain types
- Joins with pg_catalog.pg_namespace for schema information
- Uses complex subqueries to resolve collation names and aggregate check constraints
- When showSystem is false, excludes pg_catalog and information_schema
- Returns false on query validation or execution failure, true on success
- Output is ordered by schema name then domain name for consistent presentation
- Uses pg_catalog.pg_type_is_visible() for visibility checks when pattern matching

## Simplified Source

```c
bool listDomains(const char *pattern, bool verbose, bool showSystem) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    initPQExpBuffer(&buf);

    // Build base query for domain information
    printfPQExpBuffer(&buf,
        "SELECT n.nspname as \"%s\",\n"
        "       t.typname as \"%s\",\n"
        "       pg_catalog.format_type(t.typbasetype, t.typtypmod) as \"%s\",\n"
        "       (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type bt\n"
        "        WHERE c.oid = t.typcollation AND bt.oid = t.typbasetype AND t.typcollation <> bt.typcollation) as \"%s\",\n"
        "       CASE WHEN t.typnotnull THEN 'not null' END as \"%s\",\n"
        "       t.typdefault as \"%s\",\n"
        "       pg_catalog.array_to_string(ARRAY(\n"
        "         SELECT pg_catalog.pg_get_constraintdef(r.oid, true) FROM pg_catalog.pg_constraint r WHERE t.oid = r.contypid AND r.contype = 'c' ORDER BY r.conname\n"
        "       ), ' ') as \"%s\"",
        gettext_noop("Schema"),
        gettext_noop("Name"),
        gettext_noop("Type"),
        gettext_noop("Collation"),
        gettext_noop("Nullable"),
        gettext_noop("Default"),
        gettext_noop("Check"));

    // Add verbose columns (ACLs and description)
    if (verbose) {
        appendPQExpBufferStr(&buf, ",\n  ");
        printACLColumn(&buf, "t.typacl");
        appendPQExpBuffer(&buf,
            ",\n       d.description as \"%s\"",
            gettext_noop("Description"));
    }

    // Add FROM clause and joins
    appendPQExpBufferStr(&buf,
        "\nFROM pg_catalog.pg_type t\n"
        "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n");

    // Add description join for verbose mode
    if (verbose)
        appendPQExpBufferStr(&buf,
            "     LEFT JOIN pg_catalog.pg_description d "
            "ON d.classoid = t.tableoid AND d.objoid = t.oid "
            "AND d.objsubid = 0\n");

    // Filter for domain types only
    appendPQExpBufferStr(&buf, "WHERE t.typtype = 'd'\n");

    // Exclude system schemas unless requested
    if (!showSystem && !pattern)
        appendPQExpBufferStr(&buf,
            "      AND n.nspname <> 'pg_catalog'\n"
            "      AND n.nspname <> 'information_schema'\n");

    // Add pattern validation
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                                "n.nspname", "t.typname", NULL,
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

    myopt.title = _("List of domains");
    myopt.translate_header = true;

    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;
}
```