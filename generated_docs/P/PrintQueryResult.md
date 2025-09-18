# PrintQueryResult

## Location
src/bin/psql/common.c: 1004 - 1081

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
  - PQresultStatus (determines result status type)
  - StoreQueryTuple (handles \gset variable storage)
  - ExecQueryTuples (handles \gexec command execution)
  - PrintResultInCrosstab (handles \crosstabview display)
  - PrintQueryTuples (handles normal result display)
  - PrintQueryStatus (prints command status messages)
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
  - DescribeQuery (in src/bin/psql/common.c:1406)
  - ExecQueryAndProcessResults (in src/bin/psql/common.c:1795)

## Notes and Other Information
- This is a static function internal to psql's common.c module
- The function implements the core result processing logic for all psql commands
- COPY results are assumed to have been processed already by HandleCopyResult
- The 'last' parameter is critical for determining when to perform side effects like variable storage
- Error handling is delegated to callers through the boolean return value
- The function supports both single-result and multi-result command processing
- Status printing logic handles both data-returning and non-data-returning commands appropriately