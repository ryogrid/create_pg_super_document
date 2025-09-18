# PLy_spi_execute

## Location
src/pl/plpython/plpy_spi.c: 154 - 174

## Overview
PLy_spi_execute is a PL/Python dispatcher function that provides a unified interface for executing both direct SQL queries and prepared plans through the plpy.execute() method.

## Definition


## Detailed Description
This function serves as a polymorphic entry point for the  interface in PL/Python. It examines the provided arguments to determine whether the user is executing a raw SQL query string or a previously prepared plan object. Based on the argument types, it dispatches to the appropriate specialized execution function:

- If a string is provided as the first argument, it calls PLy_spi_execute_query() for direct query execution
- If a PLyPlanObject is provided, it calls PLy_spi_execute_plan() for prepared plan execution

The function uses Python's argument parsing to handle the different calling conventions and provides clear error messages when invalid arguments are provided.

## Parameters / Member Variables
- : The Python module object (unused in this context)
- : Python tuple that can contain either:
  - Format 1:  where query_string is a SQL string and limit is optional
  - Format 2:  where plan_object is a prepared plan, values_list contains parameter values, and limit is optional

## Dependencies
- Functions called/Symbols referenced:
  - PLy_spi_execute_query: Executes direct SQL query strings
  - PLy_spi_execute_plan: Executes prepared plan objects
  - is_PLyPlanObject: Type checking function for plan objects
  - PLy_exception_set: Sets Python exceptions for error reporting
- Called from (representative examples):
  - Python code via plpy.execute() interface

## Notes and Other Information
- Acts as a dispatcher function, providing a single entry point for different execution modes
- Uses PyArg_ParseTuple twice with different format strings to handle polymorphic arguments
- Clears Python errors between parsing attempts to handle the different argument formats
- Provides clear error messages when neither query string nor plan object is provided
- The limit parameter is optional in both calling conventions, defaulting to 0 (no limit)
- This design allows the same Python function (plpy.execute) to work with both raw queries and prepared statements