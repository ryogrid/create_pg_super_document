# listSchemas

## Location
[src/bin/psql/describe.c:5026-5146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5026-L5146)

## Overview
The  function implements the  psql command for displaying schema (namespace) information in a PostgreSQL database.

## Definition

```c
bool
listSchemas(const char *pattern, bool verbose, bool showSystem)
```
## Detailed Description
This function queries the  system catalog to retrieve and display information about schemas defined in the database. Schemas are logical containers that organize database objects like tables, views, functions, and types. The function shows schema names, owners, and optionally access control lists (ACLs) and descriptions.

The function includes special handling for PostgreSQL 15+ to show publication information when a specific schema pattern is provided. It queries the publication system catalogs to display which publications include the schema, providing useful information for logical replication setups.

The query can optionally exclude system schemas (those starting with 'pg_' and 'information_schema') and supports pattern matching for schema names.

## Parameters / Member Variables
- `*pattern`: A SQL name pattern (with optional wildcards) to filter which schemas to display. If NULL, all visible schemas are shown. When a specific pattern is provided in PostgreSQL 15+, publication information is also retrieved.
- `verbose`: If true, includes access control lists (permissions) and schema descriptions from the  catalog in the output.
- `showSystem`: If true, includes system schemas ('pg_*' and 'information_schema'); if false, excludes them (unless a pattern is specified).
## Dependencies
- Functions called/Symbols referenced:
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Appends formatted text to the buffer
  - : Formats access control list (ACL) information for display
  - : Validates and processes SQL name patterns with wildcards
  - : Executes the constructed SQL query
  - : Formats and displays the query results
  - : Cleans up the string buffer
  - : Allocates memory for footer strings
  - : Frees allocated memory
  - : Gets the number of result rows
  - : Gets a specific field value from the result
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Uses error handling with goto for cleanup on validation failures
- System schema filtering logic: excludes schemas matching '^pg_' regex pattern and 'information_schema'
- For PostgreSQL 15+, when a specific pattern is provided, displays publication information as footers
- [Publication](../P/Publication.md) footer shows which publications include the matched schema for logical replication
- Memory management includes proper cleanup of dynamically allocated footer strings
- Results are ordered by schema name
- ACL information shows permissions granted to different roles when verbose mode is enabled
- The function handles both single schema queries (with publications) and general schema listings

## Simplified Source

```c
bool listSchemas(const char *pattern, bool verbose, bool showSystem) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;
    int pub_schema_tuples = 0;
    char **footers = NULL;

    // Initialize query buffer
    initPQExpBuffer(&buf);

    // Build basic SELECT query for schema name and owner
    printfPQExpBuffer(&buf,
        "SELECT n.nspname AS \"Name\", "
        "pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"");

    // Add ACL and description columns in verbose mode
    if (verbose) {
        appendPQExpBufferStr(&buf, ", ");
        printACLColumn(&buf, "n.nspacl");
        appendPQExpBuffer(&buf,
            ", pg_catalog.obj_description(n.oid, 'pg_namespace') AS \"Description\"");
    }

    // Add FROM clause
    appendPQExpBufferStr(&buf, " FROM pg_catalog.pg_namespace n");

    // Filter system schemas unless explicitly requested
    if (!showSystem && !pattern) {
        appendPQExpBufferStr(&buf,
            " WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    }

    // Validate and add pattern matching
    if (!validateSQLNamePattern(&buf, pattern, !showSystem && !pattern, false,
                               NULL, "n.nspname", NULL, NULL, NULL, 2)) {
        goto error_return;
    }

    // Add ordering
    appendPQExpBufferStr(&buf, " ORDER BY 1;");

    // Execute main query
    res = PSQLexec(buf.data);
    if (!res) goto error_return;

    // Configure output options
    myopt.title = "List of schemas";
    myopt.translate_header = true;

    // For PostgreSQL 15+, add publication information if pattern specified
    if (pattern && pset.sversion >= 150000) {
        PGresult *result;

        // Query publications that include this schema
        printfPQExpBuffer(&buf,
            "SELECT pubname FROM pg_catalog.pg_publication p "
            "JOIN pg_catalog.pg_publication_namespace pn ON p.oid = pn.pnpubid "
            "JOIN pg_catalog.pg_namespace n ON n.oid = pn.pnnspid "
            "WHERE n.nspname = '%s' ORDER BY 1", pattern);

        result = PSQLexec(buf.data);
        if (!result) goto error_return;

        pub_schema_tuples = PQntuples(result);

        // Create footers for publication information
        if (pub_schema_tuples > 0) {
            footers = (char **) pg_malloc((1 + pub_schema_tuples + 1) * sizeof(char *));
            footers[0] = pg_strdup("Publications:");

            // Add each publication name to footers
            for (int i = 0; i < pub_schema_tuples; i++) {
                printfPQExpBuffer(&buf, "    \"%s\"", PQgetvalue(result, i, 0));
                footers[i + 1] = pg_strdup(buf.data);
            }
            footers[pub_schema_tuples + 1] = NULL;
            myopt.footers = footers;
        }
        PQclear(result);
    }

    // Display results
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    // Cleanup
    termPQExpBuffer(&buf);
    PQclear(res);

    // Free footer memory
    if (footers) {
        for (char **footer = footers; *footer; footer++) {
            pg_free(*footer);
        }
        pg_free(footers);
    }

    return true;

error_return:
    termPQExpBuffer(&buf);
    return false;
}
```