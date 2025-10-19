# SendQuery

## Location
[src/bin/psql/common.c:1082-1313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L1082-L1313)

## Overview
SendQuery is the main "front door" function in psql for sending user-entered queries to the PostgreSQL backend, handling transaction management, error rollback, and result processing.

## Definition

```c
bool
SendQuery(const char *query)
```
## Detailed Description
SendQuery serves as the primary interface for executing queries entered directly by users in psql. It provides comprehensive query execution management including:

- **Single-step mode verification**: When enabled, prompts user to confirm query execution
- **Echo handling**: Displays queries based on echo settings (PSQL_ECHO_QUERIES, PSQL_ECHO_ERRORS)
- **Transaction management**: Automatically begins transactions when autocommit is off and handles savepoints for error rollback
- **Query execution**: Routes to either DescribeQuery (for \gdesc) or ExecQueryAndProcessResults for normal execution
- **Error handling**: Implements sophisticated error rollback using savepoints when configured
- **Timing output**: Records and displays query execution time when timing is enabled
- **State cleanup**: Performs comprehensive cleanup of various psql state variables after execution

The function distinguishes itself from PSQLexec() by being designed for user-facing queries that are subject to single-step mode and full transaction management.

## Parameters / Member Variables
- `*query`: The SQL query string to be executed
## Dependencies
- Functions called/Symbols referenced:
  - [PQtransactionStatus](../P/PQtransactionStatus.md)
  - [command_no_begin](../c/command_no_begin.md)
  - [PQexec](../P/PQexec.md)
  - [DescribeQuery](../D/DescribeQuery.md)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md)
  - [SetCancelConn](SetCancelConn.md)/ResetCancelConn
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - [PrintTiming](../P/PrintTiming.md)
  - [PrintNotifications](../P/PrintNotifications.md)
  - [ClearOrSaveResult](../C/ClearOrSaveResult.md)
  - [clean_bind_state](../c/clean_bind_state.md)
- Called from (representative examples):
  - [MainLoop](../M/MainLoop.md) (in mainloop.c)
  - [do_copy](../d/do_copy.md) (in copy.c)
  - [ExecQueryTuples](../E/ExecQueryTuples.md) (in common.c)

## Notes and Other Information
- Returns true if query executed successfully, false otherwise
- Uses temporary savepoints (pg_psql_temporary_savepoint) for error rollback when configured
- Handles encoding changes that may occur during query execution
- Performs extensive cleanup of psql state variables including \g, \gset, \gdesc, \gexec, and \crosstabview flags
- Connected to database check is performed before execution
- Implements PostgreSQL's autocommit behavior and transaction state management

## Simplified Source

```c
bool SendQuery(const char *query) {
    bool timing = pset.timing;
    PGTransactionStatusType transaction_status;
    double elapsed_msec = 0;
    bool OK = false;
    bool on_error_rollback_savepoint = false;
    bool svpt_gone = false;

    // Check database connection
    if (!pset.db) {
        pg_log_error("You are currently not connected to a database.");
        goto sendquery_cleanup;
    }

    // Handle single-step mode
    if (pset.singlestep) {
        printf("/**(Single step mode: verify command)********************/\n%s\n/**(press return to proceed or enter x and return to cancel)***/\n", query);
        fflush(stdout);
        char buf[3];
        if (fgets(buf, sizeof(buf), stdin) != NULL && buf[0] == 'x')
            goto sendquery_cleanup;
        if (cancel_pressed)
            goto sendquery_cleanup;
    } else if (pset.echo == PSQL_ECHO_QUERIES) {
        puts(query);
        fflush(stdout);
    }

    // Log query if logging enabled
    if (pset.logfile) {
        fprintf(pset.logfile, "/******** QUERY *********/\n%s\n/************************/\n\n", query);
        fflush(pset.logfile);
    }

    SetCancelConn(pset.db);
    transaction_status = PQtransactionStatus(pset.db);

    // Begin transaction if autocommit is off
    if (transaction_status == PQTRANS_IDLE && !pset.autocommit && !command_no_begin(query)) {
        PGresult *result = PQexec(pset.db, "BEGIN");
        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
            pg_log_info("%s", PQerrorMessage(pset.db));
            ClearOrSaveResult(result);
            goto sendquery_cleanup;
        }
        ClearOrSaveResult(result);
        transaction_status = PQtransactionStatus(pset.db);
    }

    // Create savepoint for error rollback if configured
    if (transaction_status == PQTRANS_INTRANS &&
        pset.on_error_rollback != PSQL_ERROR_ROLLBACK_OFF &&
        (pset.cur_cmd_interactive || pset.on_error_rollback == PSQL_ERROR_ROLLBACK_ON)) {
        PGresult *result = PQexec(pset.db, "SAVEPOINT pg_psql_temporary_savepoint");
        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
            pg_log_info("%s", PQerrorMessage(pset.db));
            ClearOrSaveResult(result);
            goto sendquery_cleanup;
        }
        ClearOrSaveResult(result);
        on_error_rollback_savepoint = true;
    }

    // Execute query
    if (pset.gdesc_flag) {
        OK = DescribeQuery(query, &elapsed_msec);
    } else {
        OK = (ExecQueryAndProcessResults(query, &elapsed_msec, &svpt_gone, false, 0, NULL, NULL) > 0);
    }

    // Handle errors
    if (!OK && pset.echo == PSQL_ECHO_ERRORS)
        pg_log_info("STATEMENT:  %s", query);

    // Handle savepoint cleanup
    if (on_error_rollback_savepoint) {
        const char *svptcmd = NULL;
        transaction_status = PQtransactionStatus(pset.db);

        switch (transaction_status) {
            case PQTRANS_INERROR:
                svptcmd = "ROLLBACK TO pg_psql_temporary_savepoint";
                break;
            case PQTRANS_INTRANS:
                if (!svpt_gone)
                    svptcmd = "RELEASE pg_psql_temporary_savepoint";
                break;
            case PQTRANS_IDLE:
                break;
            default:
                OK = false;
                if (transaction_status != PQTRANS_UNKNOWN || ConnectionUp())
                    pg_log_error("unexpected transaction status (%d)", transaction_status);
                break;
        }

        if (svptcmd) {
            PGresult *svptres = PQexec(pset.db, svptcmd);
            if (PQresultStatus(svptres) != PGRES_COMMAND_OK) {
                pg_log_info("%s", PQerrorMessage(pset.db));
                ClearOrSaveResult(svptres);
                OK = false;
                goto sendquery_cleanup;
            }
            PQclear(svptres);
        }
    }

    // Show timing if enabled
    if (timing)
        PrintTiming(elapsed_msec);

    // Update encoding if changed
    if (pset.encoding != PQclientEncoding(pset.db) && PQclientEncoding(pset.db) >= 0) {
        pset.encoding = PQclientEncoding(pset.db);
        pset.popt.topt.encoding = pset.encoding;
        SetVariable(pset.vars, "ENCODING", pg_encoding_to_char(pset.encoding));
    }

    PrintNotifications();

sendquery_cleanup:
    ResetCancelConn();

    // Clean up various psql state flags and variables
    if (pset.gfname) {
        free(pset.gfname);
        pset.gfname = NULL;
    }
    if (pset.gsavepopt) {
        restorePsetInfo(&pset.popt, pset.gsavepopt);
        pset.gsavepopt = NULL;
    }
    clean_bind_state();
    if (pset.gset_prefix) {
        free(pset.gset_prefix);
        pset.gset_prefix = NULL;
    }
    pset.gdesc_flag = false;
    pset.gexec_flag = false;
    pset.crosstab_flag = false;

    // Clean up crosstab arguments
    for (int i = 0; i < lengthof(pset.ctv_args); i++) {
        pg_free(pset.ctv_args[i]);
        pset.ctv_args[i] = NULL;
    }

    return OK;
}
```

This simplified version preserves the essential workflow: connection validation, single-step handling, transaction management with BEGIN and savepoints, query execution, error handling, timing, encoding updates, notifications, and comprehensive state cleanup.