# pltcl_process_SPI_result

## Location
src/pl/tcl/pltcl.c: 2434 - 2546

## Overview
Processes the results from SPI_execute or SPI_execute_plan operations, converting PostgreSQL query results into appropriate Tcl interpreter values and handling optional loop body evaluation for result sets.

## Definition
```c
static int pltcl_process_SPI_result(Tcl_Interp *interp, const char *arrayname, Tcl_Obj *loop_body, int spi_rc, SPITupleTable *tuptable, uint64 ntuples)
```

## Detailed Description
This function is the shared result processing logic for SPI operations in PL/Tcl. It handles different SPI result codes and converts the results into appropriate Tcl values. For modification operations (INSERT, UPDATE, DELETE, MERGE), it returns the number of affected tuples. For SELECT operations, it can either set variables from the first tuple or iterate over all tuples, evaluating a provided Tcl loop body for each tuple.

The function supports two modes of operation: simple variable setting (when no loop body is provided) and iterative processing (when a loop body is provided). It properly handles Tcl control flow statements (continue, break, return) within loop bodies and manages memory by freeing the SPI tuple table.

## Parameters / Member Variables
- `interp`: The Tcl interpreter where results should be set and loop bodies evaluated
- `arrayname`: Name of the Tcl array where tuple column values should be stored
- `loop_body`: Optional Tcl script to execute for each result tuple (NULL for simple mode)
- `spi_rc`: The result code from the SPI operation
- `tuptable`: The SPI tuple table containing query results
- `ntuples`: Number of tuples returned by the SPI operation

## Dependencies
- Functions called/Symbols referenced:
  - pltcl_set_tuple_values
  - SPI_result_code_string
  - SPI_freetuptable
  - Tcl_SetObjResult
  - Tcl_NewWideIntObj
  - Tcl_NewIntObj
  - Tcl_AppendResult
  - Tcl_EvalObjEx
- Called from (representative examples):
  - pltcl_SPI_execute (referenced indirectly through TclExceptionNameMap)
  - pltcl_SPI_execute_plan (referenced indirectly through TclExceptionNameMap)

## Notes and Other Information
- Shared code between pltcl_SPI_execute and pltcl_SPI_execute_plan functions
- Handles all standard SPI result codes with appropriate Tcl responses
- Supports Tcl control flow (TCL_OK, TCL_CONTINUE, TCL_BREAK, TCL_RETURN, TCL_ERROR)
- Always frees the SPI tuple table to prevent memory leaks
- For SELECT-type operations, processes tuples through pltcl_set_tuple_values
- Returns TCL_OK on success, TCL_ERROR on failure, or TCL_RETURN if loop body returns
- Located in src/pl/tcl/pltcl.c:2434-2546
- Handles both single-tuple and multi-tuple result processing modes