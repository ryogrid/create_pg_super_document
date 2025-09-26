# pltcl_SPI_execute_plan

## Location
src/pl/tcl/pltcl.c: 2675 - 2695

## Overview
A Tcl command handler that executes previously prepared SQL plans within PL/Tcl functions, providing parameterized query execution capabilities.

## Definition
```c
static int
pltcl_SPI_execute_plan(ClientData cdata, Tcl_Interp *interp,
                       int objc, Tcl_Obj *const objv[])
```

## Detailed Description
This function implements the `spi_execp` command available to PL/Tcl procedures, enabling execution of previously prepared SQL plans with parameter binding. It complements `pltcl_SPI_execute` by providing efficient execution of parameterized queries that have been prepared using `spi_prepare`.

The function accepts a query identifier (prepared plan key), optional parameter values, and various execution options. It handles parameter binding with proper type conversion, supports null value specification, and processes results similarly to `pltcl_SPI_execute`. All execution occurs within a subtransaction for proper error handling.

## Parameters / Member Variables
- `cdata`: Client data passed by Tcl (unused in this implementation)
- `interp`: Tcl interpreter instance where the command is being executed
- `objc`: Number of arguments passed to the command
- `objv[]`: Array of Tcl objects representing the command arguments
  - `objv[0]`: Command name (`spi_execp`)
  - Optional `-count n`: Limit number of rows to process
  - Optional `-array name`: Store results in named Tcl array
  - Optional `-nulls string`: Specify null values for parameters ('n' for null, any other char for non-null)
  - Required query identifier (plan key)
  - Optional parameter values list (required if plan has parameters)
  - Optional loop body for processing each result row

## Dependencies
- Functions called/Symbols referenced:
  - `SPI_execute_plan`: Core SPI function for executing prepared plans
  - `pltcl_process_SPI_result`: Processes and formats SPI execution results
  - `pltcl_subtrans_begin`: Begins a subtransaction for error safety
  - `pltcl_subtrans_commit`: Commits the subtransaction on success
  - `pltcl_subtrans_abort`: Aborts subtransaction on error
  - `InputFunctionCall`: Converts Tcl strings to PostgreSQL datums
  - `Tcl_FindHashEntry`: Looks up prepared plan by identifier
  - `Tcl_ListObjGetElements`: Parses parameter value list
- Called from (representative examples):
  - `pltcl_init_interp`: Registers this function as `spi_execp` command

## Notes and Other Information
- The function is registered as the `spi_execp` command in PL/Tcl interpreters
- Requires a query to be previously prepared using `spi_prepare`
- Supports parameterized queries with proper type conversion using input functions
- The `-nulls` option allows fine-grained control over null parameter values
- Parameter count validation ensures argument list matches prepared plan requirements
- Uses UTF conversion macros for proper character encoding between Tcl and PostgreSQL
- Executes within subtransactions for robust error handling and recovery
- Memory management includes proper cleanup of argument value arrays