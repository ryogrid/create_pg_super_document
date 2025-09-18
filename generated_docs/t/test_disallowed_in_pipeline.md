# test_disallowed_in_pipeline

## Location
src/test/modules/libpq_pipeline/libpq_pipeline.c: 409 - 468

## Overview
Tests error handling and validation for operations that are not allowed in PostgreSQL pipeline mode, ensuring proper error messages and mode transitions.

## Definition


## Detailed Description
The  function validates that certain libpq operations are properly restricted when the connection is in pipeline mode, and that appropriate error messages are returned when these restrictions are violated. It also tests the pipeline mode state transitions and ensures that re-entering and exiting pipeline mode works correctly.

The function performs the following test sequence:
1. Verifies the connection is in blocking mode initially
2. Enters pipeline mode and confirms the mode change
3. Tests that PQexec() fails with the expected error message in pipeline mode
4. Tests that PQsendQuery() fails with the expected error message in pipeline mode
5. Verifies that re-entering pipeline mode is allowed (no-op)
6. Checks PQisBusy() behavior in idle pipeline mode
7. Exits pipeline mode and confirms the mode change
8. Tests that exiting pipeline mode when not in pipeline mode is a no-op
9. Verifies that PQexec() works again after exiting pipeline mode

## Parameters / Member Variables
- : The database connection to test pipeline mode restrictions on

## Dependencies
- Functions called/Symbols referenced:
  - [PQisnonblocking](../P/PQisnonblocking.md)
  - PQenterPipelineMode
  - [PQpipelineStatus](../P/PQpipelineStatus.md)
  - PQ_PIPELINE_OFF
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - PGRES_FATAL_ERROR
  - PGRES_TUPLES_OK
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [PQsendQuery](../P/PQsendQuery.md)
  - [PQisBusy](../P/PQisBusy.md)
  - PQexitPipelineMode
  - strcmp
  - fprintf
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function within the libpq_pipeline test module
- Tests critical error handling paths for pipeline mode restrictions
- Validates specific error message content to ensure proper user feedback
- Ensures pipeline mode state transitions work correctly
- Verifies that certain operations become available again after exiting pipeline mode
- Uses strcmp() to validate exact error message content
- Part of the comprehensive pipeline mode test suite
- Located in src/test/modules/libpq_pipeline/libpq_pipeline.c at lines 409-468
- Essential for ensuring pipeline mode safety and proper API usage