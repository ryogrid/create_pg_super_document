# pltcl_process_SPI_result

## Location
[src/pl/tcl/pltcl.c:2434-2546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L2434-L2546)

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
  - [pltcl_set_tuple_values](pltcl_set_tuple_values.md)
  - [SPI_result_code_string](../S/SPI_result_code_string.md)
  - [SPI_freetuptable](../S/SPI_freetuptable.md)
  - Tcl_SetObjResult
  - Tcl_NewWideIntObj
  - Tcl_NewIntObj
  - Tcl_AppendResult
  - Tcl_EvalObjEx
- Called from (representative examples):
  - [pltcl_SPI_execute](pltcl_SPI_execute.md) (referenced indirectly through TclExceptionNameMap)
  - [pltcl_SPI_execute_plan](pltcl_SPI_execute_plan.md) (referenced indirectly through TclExceptionNameMap)

## Notes and Other Information
- Shared code between pltcl_SPI_execute and pltcl_SPI_execute_plan functions
- Handles all standard SPI result codes with appropriate Tcl responses
- Supports Tcl control flow (TCL_OK, TCL_CONTINUE, TCL_BREAK, TCL_RETURN, TCL_ERROR)
- Always frees the SPI tuple table to prevent memory leaks
- For SELECT-type operations, processes tuples through pltcl_set_tuple_values
- Returns TCL_OK on success, TCL_ERROR on failure, or TCL_RETURN if loop body returns
- Located in src/pl/tcl/pltcl.c:2434-2546
- Handles both single-tuple and multi-tuple result processing modes

## Simplified Source

```c
static int pltcl_process_SPI_result(Tcl_Interp *interp, const char *arrayname,
                                   Tcl_Obj *loop_body, int spi_rc,
                                   SPITupleTable *tuptable, uint64 ntuples) {
    int my_rc = TCL_OK;
    HeapTuple *tuples;
    TupleDesc tupdesc;

    switch (spi_rc) {
        // DML operations - return number of affected rows
        case SPI_OK_SELINTO:
        case SPI_OK_INSERT:
        case SPI_OK_DELETE:
        case SPI_OK_UPDATE:
        case SPI_OK_MERGE:
            Tcl_SetObjResult(interp, Tcl_NewWideIntObj(ntuples));
            break;

        // Utility commands
        case SPI_OK_UTILITY:
        case SPI_OK_REWRITTEN:
            if (tuptable == NULL) {
                Tcl_SetObjResult(interp, Tcl_NewIntObj(0));
                break;
            }
            // Fall through for utilities returning tuples

        // SELECT and RETURNING operations
        case SPI_OK_SELECT:
        case SPI_OK_INSERT_RETURNING:
        case SPI_OK_DELETE_RETURNING:
        case SPI_OK_UPDATE_RETURNING:
        case SPI_OK_MERGE_RETURNING:
            tuples = tuptable->vals;
            tupdesc = tuptable->tupdesc;

            if (loop_body == NULL) {
                // Simple mode: set variables from first tuple only
                if (ntuples > 0)
                    pltcl_set_tuple_values(interp, arrayname, 0, tuples[0], tupdesc);
            } else {
                // Loop mode: process all tuples with loop body
                for (uint64 i = 0; i < ntuples; i++) {
                    pltcl_set_tuple_values(interp, arrayname, i, tuples[i], tupdesc);

                    int loop_rc = Tcl_EvalObjEx(interp, loop_body, 0);

                    // Handle Tcl control flow
                    if (loop_rc == TCL_OK || loop_rc == TCL_CONTINUE)
                        continue;
                    if (loop_rc == TCL_RETURN) {
                        my_rc = TCL_RETURN;
                        break;
                    }
                    if (loop_rc == TCL_BREAK)
                        break;

                    my_rc = TCL_ERROR;
                    break;
                }
            }

            if (my_rc == TCL_OK)
                Tcl_SetObjResult(interp, Tcl_NewWideIntObj(ntuples));
            break;

        default:
            Tcl_AppendResult(interp, "pltcl: SPI_execute failed: ",
                           SPI_result_code_string(spi_rc), NULL);
            my_rc = TCL_ERROR;
            break;
    }

    SPI_freetuptable(tuptable);
    return my_rc;
}
```