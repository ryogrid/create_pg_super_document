# PLy_spi_execute_plan

## Location
src/pl/plpython/plpy_spi.c: 175 - 297

## Overview
PLy_spi_execute_plan executes a previously prepared SQL plan with provided parameter values, handling parameter conversion and result processing within a managed subtransaction context.

## Definition


## Detailed Description
This function executes a prepared SQL plan (PLyPlanObject) with the provided parameter values. It performs comprehensive argument validation, ensuring the number of provided parameters matches the plan's expectations. The function creates a temporary memory context for parameter conversion, converts Python values to PostgreSQL Datum values using the type information stored in the plan, and executes the plan through SPI_execute_plan.

The execution occurs within a subtransaction to provide proper error handling and resource cleanup. Parameter conversion is handled through PLy_output_convert, which uses the pre-configured conversion functions set up when the plan was prepared. The function respects the read-only status of the current procedure and applies any specified row limit.

## Parameters / Member Variables
- : PLyPlanObject containing the prepared plan and parameter type information
- : Python sequence containing parameter values (can be NULL for parameterless plans)
- : Maximum number of rows to return (0 for no limit)

## Dependencies
- Functions called/Symbols referenced:
  - PLy_current_execution_context: Gets current execution context
  - PLy_exception_set/PLy_exception_set_plural: Error reporting functions
  - PLy_elog: PL/Python logging function
  - PLyUnicode_AsString: String conversion utility
  - PLy_spi_subtransaction_begin/commit/abort: Subtransaction management
  - AllocSetContextCreate: Creates temporary memory context
  - PLy_output_convert: Converts Python values to PostgreSQL Datum
  - SPI_execute_plan: PostgreSQL SPI function to execute prepared plan
  - PLy_spi_execute_fetch_result: Processes and returns execution results
  - MemoryContextDelete: Cleans up temporary memory context
- Called from (representative examples):
  - PLy_spi_execute: When executing plan objects through plpy.execute()
  - PLy_plan_execute: Direct plan execution method

## Notes and Other Information
- Validates that the number of provided parameters exactly matches the plan's requirements
- Creates a temporary memory context for parameter conversion to ensure proper cleanup
- Uses nested PG_TRY blocks to handle errors during parameter conversion while ensuring Python object reference cleanup
- Supports both parameterized and parameterless plan execution
- Respects the read-only flag of the current procedure context
- Provides detailed error messages including parameter count mismatches
- Handles NULL parameter values through the nulls array ('n' for NULL, ' ' for non-NULL)
- All parameter conversion and execution occurs within a subtransaction for atomic error handling