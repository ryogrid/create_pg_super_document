# PLy_output_convert

## Location
src/pl/plpython/plpy_typeio.c: 120 - 133

## Overview
Entry point function for converting Python objects to PostgreSQL Datum values in the PL/Python procedural language.

## Definition
```c
Datum PLy_output_convert(PLyObToDatum *arg, PyObject *val, bool *isnull)
```

## Detailed Description
PLy_output_convert serves as the outer-level entry point for any output conversion from Python objects to PostgreSQL data types in PL/Python. This function is the counterpart to PLy_input_convert, handling the reverse conversion direction.

The function is a thin wrapper that delegates the actual conversion work to the appropriate conversion function specified in the PLyObToDatum structure. It explicitly passes `false` as the fourth parameter to indicate that at the outer level, the conversion is not considering an array element, which affects how certain data types are processed.

Unlike PLy_input_convert, this function does not manage memory contexts explicitly - it relies on the caller to handle memory cleanup of both the result and any intermediate allocations generated during the conversion process.

## Parameters / Member Variables
- `arg`: PLyObToDatum structure containing the conversion function and metadata for the target PostgreSQL data type
- `val`: The Python object to be converted to a PostgreSQL Datum
- `isnull`: Pointer to a boolean flag that will be set to indicate if the result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [PLyObToDatum](PLyObToDatum.md) (via arg->func)
- Called from (representative examples):
  - [PLy_cursor_plan](PLy_cursor_plan.md)
  - [PLy_exec_function](PLy_exec_function.md)
  - [PLy_modify_tuple](PLy_modify_tuple.md)
  - [PLy_spi_execute_plan](PLy_spi_execute_plan.md)
  - [PLyObToDatum](PLyObToDatum.md)

## Notes and Other Information
- This function handles "output" conversion, meaning data flowing from Python back to PostgreSQL
- The caller is responsible for memory cleanup of the result and any temporary allocations
- The fourth parameter (`false`) passed to the conversion function indicates this is not an array element conversion
- This is a simple wrapper function that provides a consistent interface for output conversions
- The function works in conjunction with the PL/Python type conversion system to enable seamless data exchange between Python and PostgreSQL
- Unlike input conversion, output conversion does not use a scratch memory context for intermediate allocations