# ecpg_process_output

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1671-1943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1671-L1943)

## Overview
Processes SQL statement results and transfers data into application variables, handling various result types and output formats.

## Definition

```c
bool
ecpg_process_output(struct statement *stmt, bool clear_result)
```
## Detailed Description
This is a comprehensive result processing function that handles the complex task of transferring query results from PostgreSQL into application variables. It supports multiple output scenarios:

- **PGRES_TUPLES_OK**: Processes SELECT results into regular variables, SQL descriptors, or SQLDA structures
- **PGRES_COMMAND_OK**: Handles INSERT/UPDATE/DELETE commands, updating SQLCA with row counts and OIDs  
- **PGRES_COPY_OUT**: Manages COPY TO STDOUT operations by streaming data to stdout

The function intelligently handles different variable types (regular variables, descriptors, SQLDA), manages memory allocation/deallocation for complex structures, performs data type conversions, validates field counts, and provides comprehensive error reporting. It also processes asynchronous notifications and maintains SQLCA state information.

## Parameters / Member Variables
- `*stmt`: Pointer to statement structure containing the PGresult to process and the list of output variables to populate
- `clear_result`: Boolean flag indicating whether to call PQclear() on the result when finished (supports result reuse scenarios like cursor operations)
## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca: Gets SQLCA structure for error/status reporting
  - [PQresultStatus](../P/PQresultStatus.md): Determines result type
  - [PQnfields](../P/PQnfields.md)/PQntuples: Gets result dimensions  
  - [PQcmdStatus](../P/PQcmdStatus.md)/PQoidValue/PQcmdTuples: Gets command status info
  - [ecpg_find_desc](ecpg_find_desc.md): Locates SQL descriptors
  - [ecpg_build_compat_sqlda](ecpg_build_compat_sqlda.md)/ecpg_build_native_sqlda: Builds SQLDA structures
  - [ecpg_set_compat_sqlda](ecpg_set_compat_sqlda.md)/ecpg_set_native_sqlda: Populates SQLDA with data
  - [ecpg_store_result](ecpg_store_result.md): Transfers data to regular variables
  - [PQgetCopyData](../P/PQgetCopyData.md): Handles COPY operations
  - [PQconsumeInput](../P/PQconsumeInput.md)/PQnotifies: Processes asynchronous notifications
  - [ecpg_raise](ecpg_raise.md): Reports errors
  - [ecpg_log](ecpg_log.md): Provides debugging output
- Called from (representative examples):
  - [ecpg_do](ecpg_do.md): Main ECPG statement processing function

## Notes and Other Information
- Returns true on successful processing, false on failure
- Supports cursor readahead scenarios where function may be called repeatedly
- Handles both Informix-compatible and native PostgreSQL SQLDA formats
- Automatically manages memory for complex data structures
- Processes asynchronous notifications after main result processing
- Critical component in ECPG's data transfer pipeline between PostgreSQL and embedded applications
- Validates argument counts and raises appropriate errors for mismatches

## Simplified Source

```c
bool
ecpg_process_output(struct statement *stmt, bool clear_result)
{
    struct variable *var;
    bool status = false;
    char *cmdstat;
    struct sqlca_t *sqlca = ECPGget_sqlca();
    int nfields, ntuples, act_field;

    if (!sqlca)
    {
        ecpg_raise(stmt->lineno, ECPG_OUT_OF_MEMORY,
                   ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        return false;
    }

    var = stmt->outlist;
    switch (PQresultStatus(stmt->results))
    {
        case PGRES_TUPLES_OK:
            // Process SELECT results
            nfields = PQnfields(stmt->results);
            sqlca->sqlerrd[2] = ntuples = PQntuples(stmt->results);

            ecpg_log("ecpg_process_output on line %d: correctly got %d tuples with %d fields\n",
                     stmt->lineno, ntuples, nfields);
            status = true;

            if (ntuples < 1)
            {
                ecpg_raise(stmt->lineno, ECPG_NOT_FOUND, ECPG_SQLSTATE_NO_DATA, NULL);
                status = false;
                break;
            }

            // Handle descriptor output
            if (var && var->type == ECPGt_descriptor)
            {
                struct descriptor *desc = ecpg_find_desc(stmt->lineno, var->pointer);
                if (!desc)
                    status = false;
                else
                {
                    PQclear(desc->result);
                    desc->result = stmt->results;
                    clear_result = false;
                    ecpg_log("ecpg_process_output on line %d: putting result (%d tuples) into descriptor %s\n",
                             stmt->lineno, PQntuples(stmt->results), (const char *) var->pointer);
                }
                var = var->next;
            }
            // Handle SQLDA output (simplified - handles both Informix and native modes)
            else if (var && var->type == ECPGt_sqlda)
            {
                // Build and populate SQLDA structure based on compatibility mode
                // Creates linked list of SQLDA structures for multiple tuples
                var = var->next;
            }
            // Handle regular variable output
            else
            {
                for (act_field = 0; act_field < nfields && status; act_field++)
                {
                    if (var)
                    {
                        status = ecpg_store_result(stmt->results, act_field, stmt, var);
                        var = var->next;
                    }
                    else if (!INFORMIX_MODE(stmt->compat))
                    {
                        ecpg_raise(stmt->lineno, ECPG_TOO_FEW_ARGUMENTS,
                                  ECPG_SQLSTATE_USING_CLAUSE_DOES_NOT_MATCH_TARGETS, NULL);
                        return false;
                    }
                }
            }

            // Check for too many output variables
            if (status && var)
            {
                ecpg_raise(stmt->lineno, ECPG_TOO_MANY_ARGUMENTS,
                          ECPG_SQLSTATE_USING_CLAUSE_DOES_NOT_MATCH_TARGETS, NULL);
                status = false;
            }
            break;

        case PGRES_COMMAND_OK:
            // Process INSERT/UPDATE/DELETE results
            status = true;
            cmdstat = PQcmdStatus(stmt->results);
            sqlca->sqlerrd[1] = PQoidValue(stmt->results);
            sqlca->sqlerrd[2] = atol(PQcmdTuples(stmt->results));

            ecpg_log("ecpg_process_output on line %d: OK: %s\n", stmt->lineno, cmdstat);

            // Check for zero rows affected in data modification commands
            if (stmt->compat != ECPG_COMPAT_INFORMIX_SE &&
                !sqlca->sqlerrd[2] &&
                (strncmp(cmdstat, "UPDATE", 6) == 0 ||
                 strncmp(cmdstat, "INSERT", 6) == 0 ||
                 strncmp(cmdstat, "DELETE", 6) == 0))
                ecpg_raise(stmt->lineno, ECPG_NOT_FOUND, ECPG_SQLSTATE_NO_DATA, NULL);
            break;

        case PGRES_COPY_OUT:
            // Handle COPY TO STDOUT
            ecpg_log("ecpg_process_output on line %d: COPY OUT data transfer in progress\n",
                     stmt->lineno);

            char *buffer;
            int res;
            while ((res = PQgetCopyData(stmt->connection->connection, &buffer, 0)) > 0)
            {
                printf("%s", buffer);
                PQfreemem(buffer);
            }

            if (res == -1)
            {
                // COPY completed successfully
                PQclear(stmt->results);
                stmt->results = PQgetResult(stmt->connection->connection);
                ecpg_log("ecpg_process_output on line %d: COPY completed\n", stmt->lineno);
            }
            break;

        default:
            // Unexpected result status
            ecpg_log("ecpg_process_output on line %d: unknown execution status type\n",
                     stmt->lineno);
            ecpg_raise_backend(stmt->lineno, stmt->results,
                              stmt->connection->connection, stmt->compat);
            status = false;
            break;
    }

    // Clean up result if requested
    if (clear_result)
    {
        PQclear(stmt->results);
        stmt->results = NULL;
    }

    // Process asynchronous notifications
    PQconsumeInput(stmt->connection->connection);
    PGnotify *notify;
    while ((notify = PQnotifies(stmt->connection->connection)) != NULL)
    {
        ecpg_log("ecpg_process_output on line %d: asynchronous notification of \"%s\" from backend PID %d received\n",
                 stmt->lineno, notify->relname, notify->be_pid);
        PQfreemem(notify);
        PQconsumeInput(stmt->connection->connection);
    }

    return status;
}
```