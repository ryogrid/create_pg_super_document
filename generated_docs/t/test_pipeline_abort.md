# test_pipeline_abort

## Location
src/test/modules/libpq_pipeline/libpq_pipeline.c: 706 - 994

## Overview
Tests PostgreSQL pipeline mode error handling by verifying that when an operation in a pipeline fails, the rest of the pipeline is properly aborted and subsequent commands receive PGRES_PIPELINE_ABORTED status.

## Definition


## Detailed Description
This function comprehensively tests PostgreSQL's pipeline mode abort behavior by intentionally creating error conditions within pipelines and verifying the proper handling of aborted operations. The test creates multiple pipelines where:

1. **First Pipeline**: Contains a valid INSERT, an intentional error (calling non-existent function), and another INSERT that should be aborted
2. **Second Pipeline**: Contains a single INSERT that should execute normally after the first pipeline's error is handled
3. **Additional Tests**: Tests multiple commands in a single query string (which should error) and single-row mode with division-by-zero error

The function verifies that aborted pipeline operations return PGRES_PIPELINE_ABORTED status codes, pipeline sync operations work correctly, and that the pipeline mode state transitions are handled properly throughout the error recovery process.

## Parameters / Member Variables
- : PostgreSQL connection handle used for executing pipeline operations

## Dependencies
- Functions called/Symbols referenced:
  - PQexec (multiple calls for setup and verification)
  - PQenterPipelineMode/PQexitPipelineMode (pipeline mode control)
  - PQsendQueryParams (sending parameterized queries)
  - PQpipelineSync (pipeline synchronization)
  - PQgetResult (retrieving results)
  - PQpipelineStatus (checking pipeline state)
  - PQsetSingleRowMode (enabling single-row mode)
  - PQresultStatus/PQresStatus (result status checking)
  - PQresultErrorField (error field extraction)
  - Various PGRES_* constants (result status codes)
- Called from (representative examples):
  - main (at src/test/modules/libpq_pipeline/libpq_pipeline.c:2262)

## Notes and Other Information
- Intentionally avoids using transactions to wrap pipelines to observe individual statement effects
- Tests both successful and error scenarios within the same pipeline sequence
- Verifies that only the INSERT with value '3' (from the second pipeline) remains in the database after completion
- Includes comprehensive error checking and state validation throughout the pipeline execution
- Tests advanced features like single-row mode combined with error handling
- Part of the libpq_pipeline test module for validating PostgreSQL client library pipeline functionality