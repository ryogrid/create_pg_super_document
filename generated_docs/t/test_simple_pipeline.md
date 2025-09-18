# test_simple_pipeline

## Location
src/test/modules/libpq_pipeline/libpq_pipeline.c: 1490 - 1576

## Overview
Tests the basic functionality of PostgreSQL pipeline mode by entering pipeline mode, sending a single query, synchronizing, and properly exiting pipeline mode.

## Definition
```c
static void test_simple_pipeline(PGconn *conn)
```

## Detailed Description
This function tests the fundamental pipeline mode operations in libpq. It demonstrates the complete lifecycle of pipeline mode usage:

1. **Pipeline Mode Entry**: Enters pipeline mode using `PQenterPipelineMode()`
2. **Query Dispatch**: Sends a parameterized SELECT query using `PQsendQueryParams()`
3. **Pipeline Sync**: Issues a pipeline synchronization point with `PQpipelineSync()`
4. **Result Processing**: Retrieves and validates the query result, expecting `PGRES_TUPLES_OK`
5. **Sync Result Processing**: Retrieves and validates the sync result, expecting `PGRES_PIPELINE_SYNC`
6. **Pipeline Mode Exit**: Properly exits pipeline mode using `PQexitPipelineMode()`

The test includes several validation checks to ensure pipeline mode behaves correctly:
- Prevents exiting pipeline mode while work is in progress
- Ensures proper result sequencing (query result followed by sync result)
- Validates pipeline status throughout the process
- Confirms clean exit from pipeline mode

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (`PGconn *`) in blocking mode used for pipeline operations

## Dependencies
- Functions called/Symbols referenced:
  - PQisnonblocking - Check if connection is in non-blocking mode
  - PQenterPipelineMode - Enter pipeline mode
  - PQsendQueryParams - Send parameterized query
  - PQexitPipelineMode - Exit pipeline mode
  - PQpipelineSync - Send pipeline synchronization
  - PQgetResult - Retrieve query results
  - PQresultStatus - Get result status
  - PQresStatus - Get status string
  - PQpipelineStatus - Get current pipeline status
  - PQerrorMessage - Get error message
  - PQclear - Free result memory
  - PGRES_TUPLES_OK - Expected result status constant
  - PGRES_PIPELINE_SYNC - Expected sync result status constant
  - PQ_PIPELINE_OFF - Pipeline mode off status constant
- Called from (representative examples):
  - main - Main test driver function

## Notes and Other Information
- This is a test function specifically designed to validate basic pipeline mode functionality
- Requires a blocking connection (non-blocking mode will cause the test to fail)
- Uses a simple parameterized query (`SELECT $1` with integer parameter "1")
- Demonstrates proper error handling and validation throughout the pipeline lifecycle
- Part of the libpq_pipeline test module located in `src/test/modules/libpq_pipeline/`
- The test validates that pipeline mode cannot be exited prematurely (while work is in progress)
- Ensures that sync results are properly received and processed after query results