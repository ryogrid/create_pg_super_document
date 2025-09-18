# PLy_input_setup_tuple

## Location
[src/pl/plpython/plpy_typeio.c:165-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L165-L214)

## Overview
Initializes or re-initializes per-column input conversion information for composite/tuple types in PL/Python.

## Definition
```c
void PLy_input_setup_tuple(PLyDatumToOb *arg, TupleDesc desc, PLyProcedure *proc)
```

## Detailed Description
PLy_input_setup_tuple configures the conversion structures needed to transform PostgreSQL composite types (tuples/records) into Python dictionary objects. This function is separate from PLy_input_setup_func() because it specifically handles cases involving anonymous record types where the tuple descriptor must be passed explicitly.

The function performs several key operations:
1. Validates that the structure is set up for composite type conversion (PLyDict_FromComposite)
2. For anonymous record types (RECORDOID with negative typmod), stores a reference to the tuple descriptor
3. Allocates or reallocates the attributes array if the number of columns has changed
4. Sets up individual column conversion information for each non-dropped attribute

The function is designed to handle re-initialization efficiently, only updating conversion information for columns whose type or type modifier has changed since the last setup.

## Parameters / Member Variables
- `arg`: PLyDatumToOb structure that will be configured for tuple-to-Python conversion
- `desc`: TupleDesc describing the structure and types of the composite types columns
- `proc`: PLyProcedure context containing procedure-specific conversion settings

## Dependencies
- Functions called/Symbols referenced:
  - [PLyDict_FromComposite](PLyDict_FromComposite.md) (validation)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [PLy_input_setup_func](PLy_input_setup_func.md)
  - [pfree](../p/pfree.md)
  - TupleDescAttr
- Called from (representative examples):
  - [PLy_cursor_iternext](PLy_cursor_iternext.md)
  - [PLy_cursor_fetch](PLy_cursor_fetch.md)  
  - [PLy_exec_trigger](PLy_exec_trigger.md)
  - [PLy_spi_execute_fetch_result](PLy_spi_execute_fetch_result.md)
  - [PLyDict_FromComposite](PLyDict_FromComposite.md)

## Notes and Other Information
- This function is specifically designed to handle anonymous record types where tuple descriptors must be passed explicitly
- Only stores tuple descriptor references for anonymous record types (RECORDOID with negative typmod)
- Efficiently handles re-initialization by only updating changed column conversion information
- Memory allocation occurs in the specified memory context (arg->mcxt) for proper cleanup
- Skips dropped columns during setup to avoid unnecessary conversion overhead
- The caller is responsible for ensuring adequate lifespan of the tuple descriptor for anonymous record types
- For named composite or registered record types, the tuple descriptor does not need to be long-lived
- This function works in conjunction with PLy_input_from_tuple to provide complete tuple-to-Python conversion functionality