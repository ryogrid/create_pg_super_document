# test_singlerowmode

## Location
src/test/modules/libpq_pipeline/libpq_pipeline.c: 1577 - 1772

## Overview
Tests single-row mode and chunked row mode functionality within PostgreSQL pipeline mode, ensuring proper result handling and mode switching behavior.

## Definition
```c
static void test_singlerowmode(PGconn *conn)
```

## Detailed Description
This comprehensive test function validates single-row mode and chunked row mode operations within pipeline mode. The test consists of several distinct phases:

1. **Pipeline Setup with Single-Row Mode**: Enters pipeline mode and sends three `generate_series` queries, applying single-row mode to the first two queries only
2. **Result Processing**: Processes results, expecting `PGRES_SINGLE_TUPLE` for queries 0-1 and `PGRES_TUPLES_OK` for query 2
3. **Mode Reset Verification**: Tests that single-row mode is properly reset between queries by sending a query with single-row mode followed by a normal query
4. **Chunked Row Mode Testing**: Tests `PQsetChunkedRowsMode()` with a chunk size of 3, verifying that results are delivered in chunks of the specified size with a partial final chunk

The function validates:
- Proper single-row mode activation and result processing
- Correct result status transitions (SINGLE_TUPLE → TUPLES_OK)
- Mode isolation between queries
- Chunked row mode functionality with partial chunks
- Pipeline synchronization and proper pipeline exit

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (`PGconn *`) used for pipeline operations and result retrieval

## Dependencies
- Functions called/Symbols referenced:
  - PQenterPipelineMode - Enter pipeline mode
  - PQsendQueryParams - Send parameterized queries
  - PQpipelineSync - Send pipeline synchronization
  - PQsetSingleRowMode - Enable single-row mode
  - PQsetChunkedRowsMode - Enable chunked row mode
  - PQgetResult - Retrieve query results
  - PQsendFlushRequest - Send flush request
  - PQexitPipelineMode - Exit pipeline mode
  - PQresultStatus - Get result status
  - PQresStatus - Get status string representation
  - PQntuples - Get number of tuples in result
  - PQgetvalue - Get specific field value
  - PQclear - Free result memory
  - PQerrorMessage - Get error message
  - PGRES_SINGLE_TUPLE - Single tuple result status
  - PGRES_TUPLES_OK - Normal tuples result status
  - PGRES_TUPLES_CHUNK - Chunked tuples result status
  - PGRES_PIPELINE_SYNC - Pipeline sync result status
  - ExecStatusType - Result status enumeration type
- Called from (representative examples):
  - main - Main test driver function

## Notes and Other Information
- This is a comprehensive test function for advanced libpq result processing modes
- Tests three distinct query results using `generate_series(42, $1)` with parameters 44, 45, and 46
- Single-row mode is applied only to the first two queries, demonstrating selective mode application
- The test validates that single-row mode automatically resets between queries
- Chunked row mode test uses `generate_series(1, 5)` with chunk size 3, expecting two chunks (3 rows + 2 rows)
- Part of the libpq_pipeline test module located in `src/test/modules/libpq_pipeline/`
- Demonstrates proper error handling and result validation throughout different processing modes
- The test ensures that mode flags are properly isolated and do not affect subsequent queries
- Validates that pipeline mode can handle mixed result processing modes within a single pipeline