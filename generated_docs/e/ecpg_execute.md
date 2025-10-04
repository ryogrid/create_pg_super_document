# ecpg_execute

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1602-1670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1602-L1670)

## Overview
Executes SQL statements using the appropriate libpq function based on statement type and parameter presence.

## Definition

```c
bool
ecpg_execute(struct statement *stmt)
```
## Detailed Description
This function is the core SQL execution engine within the ECPG library. It intelligently chooses the most appropriate libpq execution function based on the statement characteristics:

- For prepared statement execution (ECPGst_execute): Uses PQexecPrepared
- For statements with parameters: Uses PQexecParams  
- For simple statements without parameters: Uses PQexec
- For PREPARE statements: Uses PQexecParams and registers the prepared statement

The function handles parameter passing, logging, error checking, and cleanup. It's designed to provide a unified interface for executing various types of SQL statements while optimizing performance based on the statement characteristics.

## Parameters / Member Variables
- `*stmt`: Pointer to statement structure containing all execution context including the SQL command, parameters, connection information, statement type, and error handling context
## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_log](ecpg_log.md): Logs execution details for debugging
  - [PQexecPrepared](../P/PQexecPrepared.md): Executes prepared statements
  - [PQexec](../P/PQexec.md): Executes simple SQL commands
  - [PQexecParams](../P/PQexecParams.md): Executes parameterized SQL commands
  - [ecpg_register_prepared_stmt](ecpg_register_prepared_stmt.md): Registers prepared statements
  - [ecpg_free_params](ecpg_free_params.md): Cleans up parameter memory
  - [ecpg_check_PQresult](ecpg_check_PQresult.md): Validates execution results
  - ECPGst_execute: Statement type constant for prepared execution
  - ECPGst_prepare: Statement type constant for PREPARE commands
- Called from (representative examples):
  - [ecpg_do](ecpg_do.md): Main ECPG statement processing function

## Notes and Other Information
- Returns true on successful execution, false on failure
- Automatically chooses optimal execution method based on statement type and parameters
- Includes comprehensive logging for debugging purposes
- Handles all parameter cleanup automatically
- Critical component in ECPG's SQL execution pipeline
- Supports both prepared and direct statement execution modes

## Simplified Source

```c
bool
ecpg_execute(struct statement *stmt)
{
    // Log execution details
    ecpg_log("ecpg_execute on line %d: query: %s; with %d parameter(s) on connection %s\n",
             stmt->lineno, stmt->command, stmt->nparams, stmt->connection->name);

    // Execute prepared statement
    if (stmt->statement_type == ECPGst_execute)
    {
        stmt->results = PQexecPrepared(stmt->connection->connection,
                                      stmt->name,
                                      stmt->nparams,
                                      (const char *const *) stmt->paramvalues,
                                      (const int *) stmt->paramlengths,
                                      (const int *) stmt->paramformats,
                                      0);
        ecpg_log("ecpg_execute on line %d: using PQexecPrepared for \"%s\"\n",
                stmt->lineno, stmt->command);
    }
    // Execute regular statement
    else
    {
        if (stmt->nparams == 0)
        {
            // Simple query without parameters
            stmt->results = PQexec(stmt->connection->connection, stmt->command);
            ecpg_log("ecpg_execute on line %d: using PQexec\n", stmt->lineno);
        }
        else
        {
            // Parameterized query
            stmt->results = PQexecParams(stmt->connection->connection,
                                        stmt->command, stmt->nparams, NULL,
                                        (const char *const *) stmt->paramvalues,
                                        (const int *) stmt->paramlengths,
                                        (const int *) stmt->paramformats,
                                        0);
            ecpg_log("ecpg_execute on line %d: using PQexecParams\n", stmt->lineno);
        }

        // Register prepared statement if this was a PREPARE command
        if (stmt->statement_type == ECPGst_prepare)
        {
            if (!ecpg_register_prepared_stmt(stmt))
            {
                ecpg_free_params(stmt, true);
                return false;
            }
        }
    }

    // Clean up parameters
    ecpg_free_params(stmt, true);

    // Check execution result
    if (!ecpg_check_PQresult(stmt->results, stmt->lineno,
                            stmt->connection->connection, stmt->compat))
        return false;

    return true;
}
```