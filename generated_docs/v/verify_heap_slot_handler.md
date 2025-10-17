# verify_heap_slot_handler

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1037-1117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1037-L1117)

## Overview
A ParallelSlotHandler function that processes and displays results from heap table verification commands in the pg_amcheck utility.

## Definition

```c
static bool
verify_heap_slot_handler(PGresult *res, PGconn *conn, void *context)
```
## Detailed Description
The  function is a callback handler that processes query results from heap table checking operations. It formats and displays verification results, including any corruption issues found in heap tables. The function handles different levels of detail in error reporting, from table-level issues down to specific block, offset, and attribute-level problems.

When verification errors are found, it outputs detailed location information (database, schema, table, block number, tuple offset, and attribute number as applicable) along with descriptive error messages. For successful queries with no errors, the function simply processes the empty result set. For failed queries, it formats and displays the error message with proper indentation.

The function also manages memory cleanup for the RelationInfo context and determines whether parallel processing should continue based on the result status.

## Parameters / Member Variables
- `*res`: PGresult pointer containing the query results from the heap verification command
- `*conn`: PGconn pointer to the database connection on which the query was executed
- `*context`: Void pointer to a RelationInfo structure containing information about the table being verified
## Dependencies
- Functions called/Symbols referenced:
  - [RelationInfo](../R/RelationInfo.md) (struct type)
  - [PQresultStatus](../P/PQresultStatus.md)
  - PGRES_TUPLES_OK
  - [PQntuples](../P/PQntuples.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - printf (with internationalization via _())
  - [indent_lines](../i/indent_lines.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - FREE_AND_SET_NULL
  - [should_processing_continue](../s/should_processing_continue.md)
- Called from:
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:776)

## Notes and Other Information
- This is a static function, only accessible within pg_amcheck.c
- Sets the global variable  to false when errors are detected
- Handles four different levels of error detail: attribute-level, tuple-level, block-level, and table-level
- Uses internationalized error messages via the _() macro
- Properly manages memory by freeing allocated strings in the RelationInfo context
- The function returns a boolean indicating whether parallel processing should continue
- Part of the parallel verification framework in pg_amcheck

## Simplified Source

```c
static bool
verify_heap_slot_handler(PGresult *res, PGconn *conn, void *context)
{
    RelationInfo *rel = (RelationInfo *) context;

    if (PQresultStatus(res) == PGRES_TUPLES_OK)
    {
        int ntups = PQntuples(res);

        if (ntups > 0)
            all_checks_pass = false;

        // Print corruption details for each tuple
        for (int i = 0; i < ntups; i++)
        {
            const char *msg = PQgetisnull(res, i, 3) ? "NO MESSAGE" : PQgetvalue(res, i, 3);

            // Print location info with increasing detail based on available fields
            if (!PQgetisnull(res, i, 2))  // Has attribute info
                printf(_("heap table \"%s.%s.%s\", block %s, offset %s, attribute %s:\n"),
                       rel->datinfo->datname, rel->nspname, rel->relname,
                       PQgetvalue(res, i, 0), PQgetvalue(res, i, 1), PQgetvalue(res, i, 2));
            else if (!PQgetisnull(res, i, 1))  // Has offset info
                printf(_("heap table \"%s.%s.%s\", block %s, offset %s:\n"),
                       rel->datinfo->datname, rel->nspname, rel->relname,
                       PQgetvalue(res, i, 0), PQgetvalue(res, i, 1));
            else if (!PQgetisnull(res, i, 0))  // Has block info
                printf(_("heap table \"%s.%s.%s\", block %s:\n"),
                       rel->datinfo->datname, rel->nspname, rel->relname,
                       PQgetvalue(res, i, 0));
            else  // Table-level error
                printf(_("heap table \"%s.%s.%s\":\n"),
                       rel->datinfo->datname, rel->nspname, rel->relname);

            printf("    %s\n", msg);
        }
    }
    else
    {
        // Handle query errors
        char *msg = indent_lines(PQerrorMessage(conn));
        all_checks_pass = false;
        printf(_("heap table \"%s.%s.%s\":\n"), rel->datinfo->datname, rel->nspname, rel->relname);
        printf("%s", msg);
        if (opts.verbose)
            printf(_("query was: %s\n"), rel->sql);
        FREE_AND_SET_NULL(msg);
    }

    // Cleanup relation info
    FREE_AND_SET_NULL(rel->sql);
    FREE_AND_SET_NULL(rel->nspname);
    FREE_AND_SET_NULL(rel->relname);

    return should_processing_continue(res);
}
```