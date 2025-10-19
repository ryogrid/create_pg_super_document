# readCommandResponse

## Location
[src/bin/pgbench/pgbench.c:3241-3382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3241-L3382)

## Overview
Processes query responses from PostgreSQL backend, handling various result types and implementing error retry logic with optional variable assignment capabilities.

## Definition
```c
static bool readCommandResponse(CState *st, MetaCommand meta, char *varprefix)
```

## Detailed Description
This comprehensive function processes query results returned from PostgreSQL, handling multiple result types including successful commands, SELECT results, pipeline synchronization, and errors. It supports META_GSET and META_ASET operations for storing query results into pgbench variables. The function implements intelligent error handling by categorizing errors and determining retry eligibility through getSQLErrorStatus() and canRetryError(). It processes all results in a loop until no more results are available, properly cleaning up resources on both success and error paths.

## Parameters / Member Variables
- `st`: Pointer to CState structure containing client connection state and execution context
- `meta`: MetaCommand enumeration specifying the type of command (META_NONE, META_GSET, META_ASET, META_ENDPIPELINE)
- `varprefix`: String prefix for variable names when storing results (required for META_GSET/META_ASET, NULL otherwise)

## Dependencies
- Functions called/Symbols referenced:
  - [PQgetResult](../P/PQgetResult.md) (retrieve query results)
  - [PQresultStatus](../P/PQresultStatus.md) (check result status)
  - [PQntuples](../P/PQntuples.md), PQnfields, PQfname, PQgetvalue (result data access)
  - [getSQLErrorStatus](../g/getSQLErrorStatus.md) (categorize SQL errors)
  - [canRetryError](../c/canRetryError.md) (determine retry eligibility)
  - [putVariable](../p/putVariable.md) (store values in pgbench variables)
  - [PQexitPipelineMode](../P/PQexitPipelineMode.md) (exit pipeline mode)
  - [commandError](../c/commandError.md) (error reporting)
  - Various PGRES_* constants and error status enums
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md)

## Notes and Other Information
- Returns true on success, false on any error condition
- Implements proper resource cleanup with PQclear() calls in error handling
- Supports PostgreSQL pipeline mode with PGRES_PIPELINE_SYNC handling
- META_GSET requires exactly one result row, while META_ASET accepts multiple rows
- Stores the last row of results for META_GSET, all rows for META_ASET
- Error retry logic only applies to serialization failures and deadlocks
- The function is static with internal linkage within pgbench.c

## Simplified Source

```c
static bool readCommandResponse(CState *st, MetaCommand meta, char *varprefix)
{
    PGresult *res;
    PGresult *next_res;
    int qrynum = 0;

    // Validate meta command and varprefix relationship
    Assert((meta == META_NONE && varprefix == NULL) ||
           ((meta == META_ENDPIPELINE) && varprefix == NULL) ||
           ((meta == META_GSET || meta == META_ASET) && varprefix != NULL));

    res = PQgetResult(st->con);

    // Process all results until none remain
    while (res != NULL)
    {
        // Check if this is the last result
        next_res = PQgetResult(st->con);
        bool is_last = (next_res == NULL);

        switch (PQresultStatus(res))
        {
            case PGRES_COMMAND_OK:
            case PGRES_EMPTY_QUERY:
                // Handle non-SELECT commands
                if (is_last && meta == META_GSET)
                {
                    pg_log_error("client %d expected one row, got %d", st->id, 0);
                    st->estatus = ESTATUS_META_COMMAND_ERROR;
                    goto error;
                }
                break;

            case PGRES_TUPLES_OK:
                // Handle SELECT results with variable assignment
                if ((is_last && meta == META_GSET) || meta == META_ASET)
                {
                    int ntuples = PQntuples(res);

                    // Validate row count for GSET
                    if (meta == META_GSET && ntuples != 1)
                    {
                        pg_log_error("client %d expected one row, got %d", st->id, ntuples);
                        st->estatus = ESTATUS_META_COMMAND_ERROR;
                        goto error;
                    }
                    else if (meta == META_ASET && ntuples <= 0)
                    {
                        break; // Skip empty results for ASET
                    }

                    // Store results in variables
                    for (int fld = 0; fld < PQnfields(res); fld++)
                    {
                        char *varname = PQfname(res, fld);

                        // Add prefix if specified
                        if (*varprefix != '\0')
                            varname = psprintf("%s%s", varprefix, varname);

                        // Store value from last row
                        if (!putVariable(&st->variables, meta == META_ASET ? "aset" : "gset",
                                         varname, PQgetvalue(res, ntuples - 1, fld)))
                        {
                            pg_log_error("client %d error storing into variable %s", st->id, varname);
                            st->estatus = ESTATUS_META_COMMAND_ERROR;
                            goto error;
                        }

                        if (*varprefix != '\0')
                            pg_free(varname);
                    }
                }
                break;

            case PGRES_PIPELINE_SYNC:
                // Handle pipeline synchronization
                pg_log_debug("client %d pipeline ending, ongoing syncs: %d", st->id, st->num_syncs);
                st->num_syncs--;
                if (st->num_syncs == 0 && PQexitPipelineMode(st->con) != 1)
                    pg_log_error("client %d failed to exit pipeline mode", st->id);
                break;

            case PGRES_NONFATAL_ERROR:
            case PGRES_FATAL_ERROR:
                // Handle retryable errors
                st->estatus = getSQLErrorStatus(PQresultErrorField(res, PG_DIAG_SQLSTATE));
                if (canRetryError(st->estatus))
                {
                    if (verbose_errors)
                        commandError(st, PQerrorMessage(st->con));
                    goto error;
                }
                // Fall through for non-retryable errors

            default:
                pg_log_error("client %d aborted in command %d query %d",
                             st->id, st->command, qrynum);
                goto error;
        }

        PQclear(res);
        qrynum++;
        res = next_res;
    }

    // Ensure we processed at least one result
    if (qrynum == 0)
    {
        pg_log_error("client %d command %d: no results", st->id, st->command);
        return false;
    }

    return true;

error:
    // Clean up resources on error
    PQclear(res);
    PQclear(next_res);
    do
    {
        res = PQgetResult(st->con);
        PQclear(res);
    } while (res);

    return false;
}
```