# PLy_input_from_tuple

## Location
[src/pl/plpython/plpy_typeio.c:134-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L134-L164)

## Overview
Converts a PostgreSQL tuple (row) into a Python dictionary object in the PL/Python procedural language.

## Definition
```c
PyObject *PLy_input_from_tuple(PLyDatumToOb *arg, HeapTuple tuple, TupleDesc desc, bool include_generated)
```

## Detailed Description
PLy_input_from_tuple transforms a PostgreSQL tuple into a Python dictionary object, providing a convenient way to access row data from Python code in PL/Python functions. The function follows the same memory management pattern as PLy_input_convert, performing all conversion work within a scratch memory context to prevent memory leaks.

The function delegates the actual conversion work to PLyDict_FromTuple, which handles the detailed process of extracting individual column values from the tuple and converting them to appropriate Python objects. The resulting dictionary maps column names to their converted Python values.

The function requires that the provided TupleDesc matches the one used during the setup of the PLyDatumToOb structure, though it doesnt enforce this requirement directly for performance reasons, relying on callers to provide the correct descriptor.

## Parameters / Member Variables
- `arg`: PLyDatumToOb structure containing conversion metadata and functions for the tuples columns
- `tuple`: PostgreSQL HeapTuple containing the row data to be converted
- `desc`: TupleDesc describing the structure and types of the tuples columns
- `include_generated`: Boolean flag indicating whether generated columns should be included in the result dictionary

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_current_execution_context](PLy_current_execution_context.md)
  - [PLy_get_scratch_context](PLy_get_scratch_context.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [PLyDict_FromTuple](PLyDict_FromTuple.md)
- Called from (representative examples):
  - [PLy_cursor_iternext](PLy_cursor_iternext.md)
  - [PLy_cursor_fetch](PLy_cursor_fetch.md)
  - [PLy_trigger_build_args](PLy_trigger_build_args.md)
  - [PLy_spi_execute_fetch_result](PLy_spi_execute_fetch_result.md)

## Notes and Other Information
- Uses the same scratch memory context strategy as PLy_input_convert to ensure proper memory management
- The TupleDesc parameter must match the one used during PLyDatumToOb setup, though this is not validated at runtime
- The include_generated parameter controls whether generated columns are included in the output dictionary
- This function is commonly used in cursor operations, trigger functions, and SPI result processing
- The resulting Python dictionary provides named access to tuple column values, making it convenient for Python code to work with PostgreSQL row data
- Memory context switching ensures that temporary allocations during tuple conversion are properly managed
- The function is part of the broader PL/Python type conversion system that enables seamless data exchange between PostgreSQL and Python

## Simplified Source

```c
PyObject *PLy_input_from_tuple(PLyDatumToOb *arg, HeapTuple tuple, TupleDesc desc, bool include_generated) {
    PyObject *dict;
    PLyExecutionContext *exec_ctx = PLy_current_execution_context();
    MemoryContext scratch_context = PLy_get_scratch_context(exec_ctx);
    MemoryContext oldcontext;

    // Reset scratch context for clean memory management
    MemoryContextReset(scratch_context);

    // Switch to scratch context for tuple conversion work
    oldcontext = MemoryContextSwitchTo(scratch_context);

    // Convert tuple to Python dictionary
    dict = PLyDict_FromTuple(arg, tuple, desc, include_generated);

    // Restore original memory context
    MemoryContextSwitchTo(oldcontext);

    return dict;
}
```