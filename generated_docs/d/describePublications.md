# describePublications

## Location
[src/bin/psql/describe.c:6339-6524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6339-L6524)

## Overview
Provides detailed descriptions of PostgreSQL logical replication publications, including their properties and associated tables/schemas, implementing the psql \dRp+ meta-command functionality.

## Definition

```c
bool
describePublications(const char *pattern)
```
## Detailed Description
The  function implements the  psql meta-command to display comprehensive information about logical replication publications. Unlike  which shows a simple list, this function provides detailed descriptions for each publication including:

1. Publication properties (owner, replication settings)
2. Individual tables published (with optional column lists and WHERE clauses for PostgreSQL 15+)
3. Schemas published (for PostgreSQL 15+)

The function dynamically adapts its output based on PostgreSQL server version:
- PostgreSQL 10+: Basic publication support
- PostgreSQL 11+: Truncate operations support
- PostgreSQL 13+: Via root partitioning support  
- PostgreSQL 15+: Column lists, WHERE clauses, and schema-level publications

For each publication found, it creates a detailed table showing all properties and then adds footer sections listing associated tables and schemas.

## Parameters / Member Variables
- `*pattern`: Optional regular expression pattern to filter publications by name. If NULL, all publications are described.
## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printTableInit](../p/printTableInit.md)
  - [printTableAddHeader](../p/printTableAddHeader.md)
  - [printTableAddCell](../p/printTableAddCell.md)
  - [addFooterToPublicationDesc](../a/addFooterToPublicationDesc.md)
  - [printTable](../p/printTable.md)
  - [printTableCleanup](../p/printTableCleanup.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c for \dRp+ command processing)

## Notes and Other Information
- Requires PostgreSQL 10.0 or later (publications were introduced in version 10)
- Creates separate detailed descriptions for each publication found
- Uses helper function  to format table and schema lists
- Handles version-specific features gracefully:
  - Column-level publications (PostgreSQL 15+)
  - WHERE clause filtering (PostgreSQL 15+) 
  - Schema-level publications (PostgreSQL 15+)
  - Truncate operation support (PostgreSQL 11+)
  - Via root partitioning (PostgreSQL 13+)
- Returns false and displays error message if no publications are found
- Uses proper error handling with cleanup for partial failures
- Each publication is displayed as a separate table with footer sections for related objects

## Simplified Source

```c
bool describePublications(const char *pattern) {
    PQExpBufferData buf;
    PGresult *res;

    // Check server version - publications require PostgreSQL 10+
    if (pset.sversion < 100000) {
        pg_log_error("The server does not support publications.");
        return true;
    }

    // Set version-specific feature flags
    bool has_pubtruncate = (pset.sversion >= 110000);
    bool has_pubviaroot = (pset.sversion >= 130000);

    initPQExpBuffer(&buf);

    // Build query to get publication basic info
    printfPQExpBuffer(&buf,
        "SELECT oid, pubname, "
        "pg_catalog.pg_get_userbyid(pubowner) AS owner, "
        "puballtables, pubinsert, pubupdate, pubdelete");

    if (has_pubtruncate)
        appendPQExpBufferStr(&buf, ", pubtruncate");
    if (has_pubviaroot)
        appendPQExpBufferStr(&buf, ", pubviaroot");

    appendPQExpBufferStr(&buf, " FROM pg_catalog.pg_publication");

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                                NULL, "pubname", NULL, NULL, NULL, 1)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, " ORDER BY 2;");

    // Execute query
    res = PSQLexec(buf.data);
    if (!res) {
        termPQExpBuffer(&buf);
        return false;
    }

    // Check if any publications found
    if (PQntuples(res) == 0) {
        if (!pset.quiet) {
            if (pattern)
                pg_log_error("Did not find any publication named \"%s\".", pattern);
            else
                pg_log_error("Did not find any publications.");
        }
        termPQExpBuffer(&buf);
        PQclear(res);
        return false;
    }

    // Display each publication in detail
    for (int i = 0; i < PQntuples(res); i++) {
        char *pubid = PQgetvalue(res, i, 0);
        char *pubname = PQgetvalue(res, i, 1);
        bool puballtables = strcmp(PQgetvalue(res, i, 3), "t") == 0;

        // Create publication details table
        printTableContent cont;
        PQExpBufferData title;
        initPQExpBuffer(&title);
        printfPQExpBuffer(&title, "Publication %s", pubname);

        // Set up table with appropriate columns
        int ncols = 5;
        if (has_pubtruncate) ncols++;
        if (has_pubviaroot) ncols++;

        printTableInit(&cont, &pset.popt.topt, title.data, ncols, 1);

        // Add headers and data
        printTableAddHeader(&cont, "Owner", true, 'l');
        printTableAddHeader(&cont, "All tables", true, 'l');
        printTableAddHeader(&cont, "Inserts", true, 'l');
        printTableAddHeader(&cont, "Updates", true, 'l');
        printTableAddHeader(&cont, "Deletes", true, 'l');
        if (has_pubtruncate)
            printTableAddHeader(&cont, "Truncates", true, 'l');
        if (has_pubviaroot)
            printTableAddHeader(&cont, "Via root", true, 'l');

        // Add publication data cells
        printTableAddCell(&cont, PQgetvalue(res, i, 2), false, false);
        printTableAddCell(&cont, PQgetvalue(res, i, 3), false, false);
        printTableAddCell(&cont, PQgetvalue(res, i, 4), false, false);
        printTableAddCell(&cont, PQgetvalue(res, i, 5), false, false);
        printTableAddCell(&cont, PQgetvalue(res, i, 6), false, false);
        if (has_pubtruncate)
            printTableAddCell(&cont, PQgetvalue(res, i, 7), false, false);
        if (has_pubviaroot)
            printTableAddCell(&cont, PQgetvalue(res, i, 8), false, false);

        // Add tables and schemas information if not publishing all tables
        if (!puballtables) {
            // Query specific tables
            printfPQExpBuffer(&buf,
                "SELECT n.nspname, c.relname");

            if (pset.sversion >= 150000) {
                appendPQExpBufferStr(&buf,
                    ", pg_get_expr(pr.prqual, c.oid), "
                    "(CASE WHEN pr.prattrs IS NOT NULL THEN "
                    "pg_catalog.array_to_string(...) ELSE NULL END)");
            } else {
                appendPQExpBufferStr(&buf, ", NULL, NULL");
            }

            appendPQExpBuffer(&buf,
                " FROM pg_catalog.pg_class c, "
                "pg_catalog.pg_namespace n, "
                "pg_catalog.pg_publication_rel pr "
                "WHERE c.relnamespace = n.oid "
                "AND c.oid = pr.prrelid "
                "AND pr.prpubid = '%s' "
                "ORDER BY 1,2", pubid);

            addFooterToPublicationDesc(&buf, "Tables:", false, &cont);

            // Query schemas (PostgreSQL 15+)
            if (pset.sversion >= 150000) {
                printfPQExpBuffer(&buf,
                    "SELECT n.nspname "
                    "FROM pg_catalog.pg_namespace n "
                    "JOIN pg_catalog.pg_publication_namespace pn ON n.oid = pn.pnnspid "
                    "WHERE pn.pnpubid = '%s' "
                    "ORDER BY 1", pubid);

                addFooterToPublicationDesc(&buf, "Tables from schemas:", true, &cont);
            }
        }

        // Display the table and cleanup
        printTable(&cont, pset.queryFout, false, pset.logfile);
        printTableCleanup(&cont);
        termPQExpBuffer(&title);
    }

    termPQExpBuffer(&buf);
    PQclear(res);
    return true;
}
```