# PrintQueryResult

## Location
[src/bin/psql/common.c:1004-1081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L1004-L1081)

## Overview
Central dispatcher function that processes query results by routing them to appropriate handlers based on result status and psql settings for printing, storing, or executing.

## Definition
static bool PrintQueryResult(PGresult *result, bool last, const printQueryOpt *opt, FILE *printQueryFout, FILE *printStatusFout)

## Detailed Description
This function serves as the primary result processor in psql, implementing a comprehensive switch statement that handles all possible PostgreSQL result statuses. Based on the result status and various psql flags, it routes results to specialized handlers like StoreQueryTuple (for \gset), ExecQueryTuples (for \gexec), PrintResultInCrosstab (for \crosstabview), or PrintQueryTuples (for normal display). The function respects the 'last' parameter to determine if this is the final result in a command sequence, which affects whether certain operations (like variable storage) should be performed.

Key routing logic:
- PGRES_TUPLES_OK: Routes to storage, execution, crosstab, or normal printing based on flags
- PGRES_COMMAND_OK: Prints status for non-data commands
- PGRES_EMPTY_QUERY: Silently succeeds
- PGRES_COPY_*: Assumes already processed (no-op)
- Error statuses: Returns failure

The function also handles status printing for INSERT/UPDATE/DELETE/MERGE RETURNING statements and respects the show_all_results setting for multi-statement commands.

## Parameters / Member Variables
- result: PGresult pointer containing the query execution result to be processed
- last: Boolean indicating if this is the final result in a command sequence (affects variable storage and some operations)
- opt: Pointer to printQueryOpt structure containing display formatting options
- printQueryFout: FILE pointer for query output, or NULL to use default
- printStatusFout: FILE pointer for status output, or NULL to use pset.queryFout

## Dependencies
- Functions called/Symbols referenced:
  - [PQresultStatus](PQresultStatus.md) (determines result status type)
  - [StoreQueryTuple](../S/StoreQueryTuple.md) (handles \gset variable storage)
  - [ExecQueryTuples](../E/ExecQueryTuples.md) (handles \gexec command execution)
  - [PrintResultInCrosstab](PrintResultInCrosstab.md) (handles \crosstabview display)
  - [PrintQueryTuples](PrintQueryTuples.md) (handles normal result display)
  - [PrintQueryStatus](PrintQueryStatus.md) (prints command status messages)
  - pg_log_error (logs unexpected errors)
- Constants referenced:
  - PGRES_TUPLES_OK (successful query with data)
  - PGRES_COMMAND_OK (successful command without data)
  - PGRES_EMPTY_QUERY (empty query string)
  - PGRES_COPY_OUT/PGRES_COPY_IN (COPY operations)
  - PGRES_BAD_RESPONSE/PGRES_NONFATAL_ERROR/PGRES_FATAL_ERROR (error statuses)
- Global variables accessed:
  - pset.gset_prefix (\gset prefix setting)
  - pset.gexec_flag (\gexec enabled flag)
  - pset.crosstab_flag (\crosstabview enabled flag)
  - pset.show_all_results (show intermediate results flag)
- Called from:
  - [DescribeQuery](../D/DescribeQuery.md) (in src/bin/psql/common.c:1406)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (in src/bin/psql/common.c:1795)

## Notes and Other Information
- This is a static function internal to psql's common.c module
- The function implements the core result processing logic for all psql commands
- COPY results are assumed to have been processed already by HandleCopyResult
- The 'last' parameter is critical for determining when to perform side effects like variable storage
- Error handling is delegated to callers through the boolean return value
- The function supports both single-result and multi-result command processing
- Status printing logic handles both data-returning and non-data-returning commands appropriately

## Simplified Source

```c
static bool PrintQueryResult(PGresult *result, bool last,
                           const printQueryOpt *opt, FILE *printQueryFout,
                           FILE *printStatusFout) {
    bool success;

    if (!result)
        return false;

    switch (PQresultStatus(result)) {
        case PGRES_TUPLES_OK:
            // Route to appropriate handler based on psql flags
            if (last && pset.gset_prefix)
                success = StoreQueryTuple(result);
            else if (last && pset.gexec_flag)
                success = ExecQueryTuples(result);
            else if (last && pset.crosstab_flag)
                success = PrintResultInCrosstab(result);
            else if (last || pset.show_all_results)
                success = PrintQueryTuples(result, opt, printQueryFout);
            else
                success = true;

            // Print status for INSERT/UPDATE/DELETE/MERGE RETURNING
            if (last || pset.show_all_results)
                PrintQueryStatus(result, printStatusFout);
            break;

        case PGRES_COMMAND_OK:
            if (last || pset.show_all_results)
                PrintQueryStatus(result, printStatusFout);
            success = true;
            break;

        case PGRES_EMPTY_QUERY:
            success = true;
            break;

        case PGRES_COPY_OUT:
        case PGRES_COPY_IN:
            // Already processed elsewhere
            success = true;
            break;

        case PGRES_BAD_RESPONSE:
        case PGRES_NONFATAL_ERROR:
        case PGRES_FATAL_ERROR:
            success = false;
            break;

        default:
            success = false;
            pg_log_error("unexpected PQresultStatus: %d", PQresultStatus(result));
            break;
    }

    return success;
}
```

This simplified version preserves the core routing logic: dispatch results to appropriate handlers based on status and psql flags, handle status printing, and return success/failure status.