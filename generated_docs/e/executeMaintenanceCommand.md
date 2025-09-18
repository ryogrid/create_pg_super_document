# executeMaintenanceCommand

## Location
src/fe_utils/query_utils.c: 74 - 91

## Overview
A utility function in PostgreSQL frontend utilities that executes maintenance SQL commands with optional echo support and cancellation handling.

## Definition


## Detailed Description
The `executeMaintenanceCommand` function is designed specifically for executing long-running maintenance operations such as CLUSTER, REINDEX, VACUUM, and ANALYZE commands. Unlike the simpler `executeCommand` function, this function provides enhanced user experience features including the ability to echo commands to stdout and most importantly, the ability to handle Ctrl-C interruptions gracefully through PostgreSQL's cancellation mechanism. The function returns a boolean indicating success or failure rather than terminating the program on error, allowing calling code to handle failures appropriately.

The function sets up a cancellation handler before executing the command and resets it afterward, ensuring that users can interrupt long-running maintenance operations without leaving the database in an inconsistent state.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle used to execute the maintenance command
- `query`: The SQL maintenance command string to execute (typically CLUSTER, REINDEX, VACUUM, etc.)
- `echo`: Boolean flag indicating whether to echo the command to stdout before execution

## Dependencies
- Functions called/Symbols referenced:
  - printf (for echoing the command when echo flag is true)
  - SetCancelConn (to enable Ctrl-C cancellation for the connection)
  - PQexec (PostgreSQL API function to execute the command)
  - ResetCancelConn (to disable cancellation handling after execution)
  - PGRES_COMMAND_OK (PostgreSQL result status constant for successful commands)
  - PQclear (to free the result set memory)
- Called from (representative examples):
  - cluster_one_database (for executing CLUSTER commands)
  - Various maintenance utilities that need cancellable operations

## Notes and Other Information
- This function is part of the fe_utils library, making it available across multiple PostgreSQL frontend utilities
- Unlike `executeCommand` and `executeQuery`, this function returns a success/failure boolean instead of terminating on error
- The cancellation mechanism allows users to interrupt long-running operations safely using Ctrl-C
- The echo parameter is useful for verbose modes in command-line utilities
- Designed specifically for maintenance operations that may take significant time to complete
- Properly manages memory by calling PQclear on the result set
- The function handles the common pattern of maintenance operations: setup cancellation, execute, cleanup cancellation, return status