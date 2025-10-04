# test_prepared

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:1254-1412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L1254-L1412)

## Overview
Tests PostgreSQL pipeline mode functionality for prepared statements and portals, including creation, description, execution, and cleanup operations within pipeline contexts.

## Definition

```c
static void
test_prepared(PGconn *conn)
```
## Detailed Description
This function comprehensively tests the pipeline mode support for prepared statements and portal operations in PostgreSQL. The test is divided into several phases:

1. **Prepared Statement Testing**: Creates a prepared statement with mixed parameter types (INT4, TEXT, NUMERIC, INTERVAL), describes it to verify column types, and then closes it
2. **Portal Testing**: Creates a cursor portal, describes it to verify its structure, and then closes it
3. **Error Handling**: Verifies that operations on closed statements/portals properly return errors
4. **Blocking vs Non-blocking**: Tests both pipeline (non-blocking) and traditional (blocking) modes for statement/portal operations

The test validates that pipeline mode correctly handles the full lifecycle of prepared statements and portals, including proper result status codes, type information retrieval, and cleanup operations. It ensures that describe operations return correct metadata and that closing operations work properly in pipeline contexts.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle for pipeline operations
## Dependencies
- Functions called/Symbols referenced:
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md)/PQexitPipelineMode (pipeline mode control)
  - [PQsendPrepare](../P/PQsendPrepare.md)/PQsendClosePrepared (prepared statement operations)
  - [PQsendDescribePrepared](../P/PQsendDescribePrepared.md)/PQdescribePrepared (statement description)
  - [PQsendDescribePortal](../P/PQsendDescribePortal.md)/PQdescribePortal (portal description)
  - [PQsendClosePortal](../P/PQsendClosePortal.md)/PQclosePortal (portal cleanup)
  - [PQpipelineSync](../P/PQpipelineSync.md) (pipeline synchronization)
  - [PQgetResult](../P/PQgetResult.md) (result retrieval)
  - [PQexec](../P/PQexec.md) (direct SQL execution for setup)
  - [PQnfields](../P/PQnfields.md)/PQftype (result metadata access)
  - PGRES_* constants (result status codes)
  - Type OIDs (INT4OID, TEXTOID, NUMERICOID, INTERVALOID)
- Called from (representative examples):
  - [main](../m/main.md) (at src/test/modules/libpq_pipeline/libpq_pipeline.c:2268)

## Notes and Other Information
- Tests complex SQL with multiple parameter types and type casting
- Validates proper metadata retrieval for prepared statements in pipeline mode
- Ensures that closing non-existent statements/portals is handled gracefully as no-ops
- Demonstrates proper pipeline synchronization after each operation phase
- Verifies that error conditions are properly reported for closed objects
- Essential for validating prepared statement support in PostgreSQL's pipeline architecture
- Part of the libpq_pipeline test suite ensuring robust prepared statement functionality

## Simplified Source

```c
static void test_prepared(PGconn *conn) {
    PGresult *res = NULL;
    Oid param_oids[1] = {INT4OID};
    Oid expected_oids[4];
    Oid typ;

    fprintf(stderr, "prepared... ");

    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

    // Test 1: Prepare and describe a statement
    if (PQsendPrepare(conn, "select_one", "SELECT $1, '42', $1::numeric, "
                      "interval '1 sec'", 1, param_oids) != 1)
        pg_fatal("preparing query failed: %s", PQerrorMessage(conn));

    expected_oids[0] = INT4OID;
    expected_oids[1] = TEXTOID;
    expected_oids[2] = NUMERICOID;
    expected_oids[3] = INTERVALOID;

    if (PQsendDescribePrepared(conn, "select_one") != 1)
        pg_fatal("failed to send describePrepared: %s", PQerrorMessage(conn));
    if (PQpipelineSync(conn) != 1)
        pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

    // Process prepare result
    res = PQgetResult(conn);
    if (res == NULL)
        pg_fatal("PQgetResult returned null");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("expected COMMAND_OK, got %s", PQresStatus(PQresultStatus(res)));
    PQclear(res);
    PQgetResult(conn); // consume NULL

    // Process describe result
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("expected COMMAND_OK, got %s", PQresStatus(PQresultStatus(res)));
    if (PQnfields(res) != 4)
        pg_fatal("expected 4 columns, got %d", PQnfields(res));

    // Verify column types
    for (int i = 0; i < PQnfields(res); i++) {
        typ = PQftype(res, i);
        if (typ != expected_oids[i])
            pg_fatal("field %d: expected type %u, got %u", i, expected_oids[i], typ);
    }
    PQclear(res);
    PQgetResult(conn); // consume NULL

    // Get sync result
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
        pg_fatal("expected PGRES_PIPELINE_SYNC, got %s", PQresStatus(PQresultStatus(res)));

    // Test 2: Close the prepared statement
    fprintf(stderr, "closing statement..");
    if (PQsendClosePrepared(conn, "select_one") != 1)
        pg_fatal("PQsendClosePrepared failed: %s", PQerrorMessage(conn));
    if (PQpipelineSync(conn) != 1)
        pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

    // Process close result
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("expected COMMAND_OK, got %s", PQresStatus(PQresultStatus(res)));
    PQclear(res);
    PQgetResult(conn); // consume NULL

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
        pg_fatal("expected PGRES_PIPELINE_SYNC, got %s", PQresStatus(PQresultStatus(res)));

    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("could not exit pipeline mode: %s", PQerrorMessage(conn));

    // Test 3: Verify statement is closed (should error)
    res = PQdescribePrepared(conn, "select_one");
    if (PQresultStatus(res) != PGRES_FATAL_ERROR)
        pg_fatal("expected FATAL_ERROR, got %s", PQresStatus(PQresultStatus(res)));

    // Test 4: Portal operations
    fprintf(stderr, "creating portal... ");
    PQexec(conn, "BEGIN");
    PQexec(conn, "DECLARE cursor_one CURSOR FOR SELECT 1");
    PQenterPipelineMode(conn);

    if (PQsendDescribePortal(conn, "cursor_one") != 1)
        pg_fatal("PQsendDescribePortal failed: %s", PQerrorMessage(conn));
    if (PQpipelineSync(conn) != 1)
        pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

    // Verify portal description
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("expected COMMAND_OK, got %s", PQresStatus(PQresultStatus(res)));

    typ = PQftype(res, 0);
    if (typ != INT4OID)
        pg_fatal("portal: expected type %u, got %u", INT4OID, typ);
    PQclear(res);
    PQgetResult(conn); // consume NULL

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
        pg_fatal("expected PGRES_PIPELINE_SYNC, got %s", PQresStatus(PQresultStatus(res)));

    // Close portal
    fprintf(stderr, "closing portal... ");
    if (PQsendClosePortal(conn, "cursor_one") != 1)
        pg_fatal("PQsendClosePortal failed: %s", PQerrorMessage(conn));
    if (PQpipelineSync(conn) != 1)
        pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

    // Process close results (simplified)
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("expected COMMAND_OK");
    PQclear(res);
    PQgetResult(conn); // consume NULL
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
        pg_fatal("expected PGRES_PIPELINE_SYNC");

    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("could not exit pipeline mode: %s", PQerrorMessage(conn));

    fprintf(stderr, "ok\n");
}
```