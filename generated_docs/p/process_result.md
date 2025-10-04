# process_result

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:2089-2136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L2089-L2136)

## Overview
Helper function for processing individual pipeline results with proper error detection and NULL result consumption, specifically designed for the test_uniqviol test.

## Definition
```c
static bool process_result(PGconn *conn, PGresult *res, int results, int numsent)
```

## Detailed Description
This utility function processes individual query results from a pipeline operation and handles the expected NULL result that follows each actual result. It performs comprehensive result validation and logging:

1. **Result Validation**: Ensures the result is not unexpectedly NULL
2. **Status-based Processing**: Handles three specific result statuses:
   - `PGRES_FATAL_ERROR`: Logs error details and sets error flag
   - `PGRES_TUPLES_OK`: Logs successful result value (expects single row/column)
   - `PGRES_PIPELINE_ABORTED`: Logs pipeline aborted status
3. **NULL Consumption**: After processing each result, retrieves and validates the expected NULL result that follows
4. **Error Reporting**: Returns a boolean indicating whether a fatal error was encountered
5. **Progress Tracking**: Logs result number and total sent count for debugging

The function is specifically designed to work with the prepared INSERT statement in `test_uniqviol` that returns the inserted ID value.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (`PGconn *`) for additional result retrieval and error messages
- `res`: The result object (`PGresult *`) to process - must not be NULL
- `results`: Current result number (0-based) for logging and tracking
- `numsent`: Total number of queries sent for progress reporting

## Dependencies
- Functions called/Symbols referenced:
  - [PQresultStatus](../P/PQresultStatus.md) - Get result status code
  - [PQgetResult](../P/PQgetResult.md) - Retrieve the expected NULL result that follows
  - [PQerrorMessage](../P/PQerrorMessage.md) - Get detailed error message for fatal errors
  - [PQgetvalue](../P/PQgetvalue.md) - Extract the returned value from successful queries
  - [PQclear](../P/PQclear.md) - Free the result memory
  - [PQresStatus](../P/PQresStatus.md) - Convert status code to string representation
  - PGRES_FATAL_ERROR - Fatal error status constant
  - PGRES_TUPLES_OK - Successful query result status
  - PGRES_PIPELINE_ABORTED - Pipeline aborted status constant
- Called from (representative examples):
  - [test_uniqviol](../t/test_uniqviol.md) - Main caller that processes multiple pipeline results

## Notes and Other Information
- This is a specialized helper function for pipeline result processing
- Assumes each non-NULL result is followed by exactly one NULL result
- Designed specifically for the unique constraint violation test scenario
- Expects successful queries to return exactly one row with one column (the ID)
- Provides detailed logging for debugging pipeline behavior
- Fatal errors (like unique constraint violations) are expected and handled gracefully
- Part of the libpq_pipeline test module located in `src/test/modules/libpq_pipeline/`
- The function enforces the libpq protocol requirement that each result be followed by NULL
- Returns boolean flag to allow caller to track whether any errors occurred
- Used in conjunction with non-blocking I/O and prepared statement execution
- Handles pipeline-aborted results that occur after errors in pipeline mode
- The logging format includes result position and total count for progress tracking

## Simplified Source

```c
static bool process_result(PGconn *conn, PGresult *res, int results, int numsent) {
    PGresult *res2;
    bool got_error = false;

    if (res == NULL)
        pg_fatal("got unexpected NULL");

    switch (PQresultStatus(res)) {
        case PGRES_FATAL_ERROR:
            got_error = true;
            fprintf(stderr, "result %d/%d (error): %s\n", results, numsent, PQerrorMessage(conn));
            PQclear(res);

            // Consume expected NULL result
            res2 = PQgetResult(conn);
            if (res2 != NULL)
                pg_fatal("expected NULL, got %s", PQresStatus(PQresultStatus(res2)));
            break;

        case PGRES_TUPLES_OK:
            fprintf(stderr, "result %d/%d: %s\n", results, numsent, PQgetvalue(res, 0, 0));
            PQclear(res);

            // Consume expected NULL result
            res2 = PQgetResult(conn);
            if (res2 != NULL)
                pg_fatal("expected NULL, got %s", PQresStatus(PQresultStatus(res2)));
            break;

        case PGRES_PIPELINE_ABORTED:
            fprintf(stderr, "result %d/%d: pipeline aborted\n", results, numsent);
            // Consume expected NULL result
            res2 = PQgetResult(conn);
            if (res2 != NULL)
                pg_fatal("expected NULL, got %s", PQresStatus(PQresultStatus(res2)));
            break;

        default:
            pg_fatal("got unexpected %s", PQresStatus(PQresultStatus(res)));
    }

    return got_error;
}
```