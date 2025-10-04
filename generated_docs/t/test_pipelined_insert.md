# test_pipelined_insert

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:1008-1253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L1008-L1253)

## Overview
Tests PostgreSQL pipeline mode with bulk insert operations using prepared statements in non-blocking mode, demonstrating efficient handling of large-scale data insertion within a transaction pipeline.

## Definition

```c
enum PipelineInsertStep send_step = BI_BEGIN_TX,
				recv_step = BI_BEGIN_TX;
```
## Detailed Description
This function performs a comprehensive test of PostgreSQL's pipeline mode for bulk insert operations. It creates a complete transaction workflow including table creation, prepared statement setup, and bulk data insertion using non-blocking I/O to avoid deadlocks. The test follows a state machine approach with distinct phases:

1. **Setup Phase**: Begins transaction, drops/creates test table, and prepares an INSERT statement
2. **Insert Phase**: Executes bulk inserts using the prepared statement with parameterized values
3. **Cleanup Phase**: Commits the transaction and synchronizes the pipeline

The function uses non-blocking mode with select() to interleave sending commands and receiving results, preventing buffer overflow scenarios that could cause deadlocks during high-volume operations. Each phase is tracked through a PipelineInsertStep state machine that coordinates both sending and receiving operations.

## Parameters / Member Variables
- : PostgreSQL connection handle for pipeline operations
- : Number of rows to insert during the bulk insert test

## Dependencies
- Functions called/Symbols referenced:
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md)/PQexitPipelineMode (pipeline mode control)
  - [PQsendQueryParams](../P/PQsendQueryParams.md) (sending SQL commands)
  - [PQsendPrepare](../P/PQsendPrepare.md) (preparing statements)
  - [PQsendQueryPrepared](../P/PQsendQueryPrepared.md) (executing prepared statements)
  - [PQsetnonblocking](../P/PQsetnonblocking.md) (enabling non-blocking I/O)
  - [PQsocket](../P/PQsocket.md)/select/FD_SET/FD_ZERO (socket-level I/O management)
  - [PQconsumeInput](../P/PQconsumeInput.md)/PQisBusy/PQgetResult (result processing)
  - [PQpipelineSync](../P/PQpipelineSync.md) (pipeline synchronization)
  - [PQflush](../P/PQflush.md) (forcing output buffer flush)
  - PipelineInsertStep enum and BI_* constants (state machine states)
  - MAXINTLEN/MAXINT8LEN (parameter formatting constants)
- Called from (representative examples):
  - [main](../m/main.md) (at src/test/modules/libpq_pipeline/libpq_pipeline.c:2266)

## Notes and Other Information
- Demonstrates proper non-blocking pipeline handling to prevent deadlocks during bulk operations
- Uses prepared statements for efficient parameter binding during bulk inserts
- Implements a state machine pattern for coordinating complex multi-phase pipeline operations
- Tests wide integer values (1LL << 62) to exercise buffer space management
- Validates proper command tag verification for each pipeline phase
- Essential test for verifying pipeline mode scalability with large data volumes
- Part of the libpq_pipeline test suite for PostgreSQL client library validation

## Simplified Source

```c
static void test_pipelined_insert(PGconn *conn, int n_rows) {
    Oid insert_param_oids[2] = {INT4OID, INT8OID};
    const char *insert_params[2];
    char insert_param_0[MAXINTLEN];
    char insert_param_1[MAXINT8LEN];
    enum PipelineInsertStep send_step = BI_BEGIN_TX, recv_step = BI_BEGIN_TX;
    int rows_to_send, rows_to_receive;

    insert_params[0] = insert_param_0;
    insert_params[1] = insert_param_1;
    rows_to_send = rows_to_receive = n_rows;

    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

    // Phase 1: Send setup commands (BEGIN, DROP TABLE, CREATE TABLE)
    while (send_step != BI_PREPARE) {
        const char *sql;
        switch (send_step) {
            case BI_BEGIN_TX:
                sql = "BEGIN TRANSACTION";
                send_step = BI_DROP_TABLE;
                break;
            case BI_DROP_TABLE:
                sql = drop_table_sql;
                send_step = BI_CREATE_TABLE;
                break;
            case BI_CREATE_TABLE:
                sql = create_table_sql;
                send_step = BI_PREPARE;
                break;
            default:
                pg_fatal("invalid state");
        }

        if (PQsendQueryParams(conn, sql, 0, NULL, NULL, NULL, NULL, 0) != 1)
            pg_fatal("dispatching %s failed: %s", sql, PQerrorMessage(conn));
    }

    // Phase 2: Prepare INSERT statement
    if (PQsendPrepare(conn, "my_insert", insert_sql2, 2, insert_param_oids) != 1)
        pg_fatal("dispatching PREPARE failed: %s", PQerrorMessage(conn));
    send_step = BI_INSERT_ROWS;

    // Phase 3: Switch to non-blocking mode for bulk operations
    if (PQsetnonblocking(conn, 1) != 0)
        pg_fatal("failed to set nonblocking mode: %s", PQerrorMessage(conn));

    // Phase 4: Main processing loop - send inserts and process results
    while (recv_step != BI_DONE) {
        int sock = PQsocket(conn);
        fd_set input_mask, output_mask;

        // Setup file descriptor sets for select()
        FD_ZERO(&input_mask);
        FD_SET(sock, &input_mask);
        FD_ZERO(&output_mask);
        FD_SET(sock, &output_mask);

        if (select(sock + 1, &input_mask, &output_mask, NULL, NULL) < 0) {
            fprintf(stderr, "select() failed: %m\n");
            exit_nicely(conn);
        }

        // Process incoming results
        if (FD_ISSET(sock, &input_mask)) {
            PQconsumeInput(conn);
            while (!PQisBusy(conn) && recv_step < BI_DONE) {
                PGresult *res = PQgetResult(conn);
                if (res == NULL)
                    continue;

                // Validate result based on current receive step
                // (details simplified for brevity)
                const char *expected_cmdtag = "";
                int expected_status = PGRES_COMMAND_OK;
                // ... status checking logic ...

                if (recv_step == BI_INSERT_ROWS) {
                    rows_to_receive--;
                    if (rows_to_receive == 0)
                        recv_step++;
                } else {
                    recv_step++;
                }

                PQclear(res);
            }
        }

        // Send more commands when output buffer is ready
        if (FD_ISSET(sock, &output_mask)) {
            PQflush(conn);

            if (send_step == BI_INSERT_ROWS && rows_to_send > 0) {
                snprintf(insert_param_0, MAXINTLEN, "%d", rows_to_send);
                snprintf(insert_param_1, MAXINT8LEN, "%lld", 1LL << 62);

                if (PQsendQueryPrepared(conn, "my_insert", 2, insert_params,
                                        NULL, NULL, 0) == 1) {
                    rows_to_send--;
                    if (rows_to_send == 0)
                        send_step++;
                }
            } else if (send_step == BI_COMMIT_TX) {
                if (PQsendQueryParams(conn, "COMMIT", 0, NULL, NULL, NULL, NULL, 0) == 1)
                    send_step++;
            } else if (send_step == BI_SYNC) {
                if (PQpipelineSync(conn) == 1)
                    send_step++;
            }
        }
    }

    // Cleanup: exit pipeline mode and restore blocking mode
    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("attempt to exit pipeline mode failed: %s", PQerrorMessage(conn));

    if (PQsetnonblocking(conn, 0) != 0)
        pg_fatal("failed to clear nonblocking mode: %s", PQerrorMessage(conn));

    fprintf(stderr, "ok\n");
}
```