# test_prepared

## Location
src/test/modules/libpq_pipeline/libpq_pipeline.c: 1254 - 1412

## Overview
Tests PostgreSQL pipeline mode functionality for prepared statements and portals, including creation, description, execution, and cleanup operations within pipeline contexts.

## Definition


## Detailed Description
This function comprehensively tests the pipeline mode support for prepared statements and portal operations in PostgreSQL. The test is divided into several phases:

1. **Prepared Statement Testing**: Creates a prepared statement with mixed parameter types (INT4, TEXT, NUMERIC, INTERVAL), describes it to verify column types, and then closes it
2. **Portal Testing**: Creates a cursor portal, describes it to verify its structure, and then closes it
3. **Error Handling**: Verifies that operations on closed statements/portals properly return errors
4. **Blocking vs Non-blocking**: Tests both pipeline (non-blocking) and traditional (blocking) modes for statement/portal operations

The test validates that pipeline mode correctly handles the full lifecycle of prepared statements and portals, including proper result status codes, type information retrieval, and cleanup operations. It ensures that describe operations return correct metadata and that closing operations work properly in pipeline contexts.

## Parameters / Member Variables
- : PostgreSQL connection handle for pipeline operations

## Dependencies
- Functions called/Symbols referenced:
  - PQenterPipelineMode/PQexitPipelineMode (pipeline mode control)
  - PQsendPrepare/PQsendClosePrepared (prepared statement operations)
  - PQsendDescribePrepared/PQdescribePrepared (statement description)
  - PQsendDescribePortal/PQdescribePortal (portal description)
  - PQsendClosePortal/PQclosePortal (portal cleanup)
  - PQpipelineSync (pipeline synchronization)
  - PQgetResult (result retrieval)
  - PQexec (direct SQL execution for setup)
  - PQnfields/PQftype (result metadata access)
  - PGRES_* constants (result status codes)
  - Type OIDs (INT4OID, TEXTOID, NUMERICOID, INTERVALOID)
- Called from (representative examples):
  - main (at src/test/modules/libpq_pipeline/libpq_pipeline.c:2268)

## Notes and Other Information
- Tests complex SQL with multiple parameter types and type casting
- Validates proper metadata retrieval for prepared statements in pipeline mode
- Ensures that closing non-existent statements/portals is handled gracefully as no-ops
- Demonstrates proper pipeline synchronization after each operation phase
- Verifies that error conditions are properly reported for closed objects
- Essential for validating prepared statement support in PostgreSQL's pipeline architecture
- Part of the libpq_pipeline test suite ensuring robust prepared statement functionality