# PLy_input_convert

## Location
src/pl/plpython/plpy_typeio.c: 81 - 119

## Overview
Entry point function for converting PostgreSQL Datum values to Python objects in the PL/Python procedural language.

## Definition
```c
PyObject *PLy_input_convert(PLyDatumToOb *arg, Datum val)
```

## Detailed Description
PLy_input_convert serves as the outer-level entry point for any input conversion from PostgreSQL data types to Python objects in PL/Python. The function implements a memory-safe conversion strategy by performing all conversion work within a scratch memory context to prevent memory leaks from datatype output function calls.

The function follows a careful memory management pattern: it resets the scratch context before each conversion cycle rather than after, which ensures that Python reference counts on result objects are properly handled even if MemoryContextReset throws an error during cleanup.

Internally, the function delegates the actual conversion work to the appropriate conversion function specified in the PLyDatumToOb structure, allowing for recursive conversions of complex data types.

## Parameters / Member Variables
- `arg`: PLyDatumToOb structure containing the conversion function and metadata for the specific PostgreSQL data type
- `val`: The PostgreSQL Datum value to be converted to a Python object

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_current_execution_context](PLy_current_execution_context.md)
  - [PLy_get_scratch_context](PLy_get_scratch_context.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [PLy_function_build_args](PLy_function_build_args.md)
  - [PLyObToDatum](PLyObToDatum.md)

## Notes and Other Information
- This function is designed to be the single entry point for all input conversions, ensuring consistent memory management across all data type conversions
- The scratch context strategy prevents memory leaks that could occur from recursive conversion calls
- The function is part of the PL/Python type conversion system, where "input" refers to data flowing from PostgreSQL into Python
- Memory context switching ensures that temporary allocations during conversion are properly cleaned up
- The conversion functions can recurse directly to each other, but this outer wrapper ensures proper memory management at the top level