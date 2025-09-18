# PLy_output_setup_tuple

## Location
src/pl/plpython/plpy_typeio.c: 215 - 260

## Overview
Initializes or re-initializes per-column output conversion information for composite/tuple types in PL/Python.

## Definition
```c
void PLy_output_setup_tuple(PLyObToDatum *arg, TupleDesc desc, PLyProcedure *proc)
```

## Detailed Description
PLy_output_setup_tuple configures the conversion structures needed to transform Python objects into PostgreSQL composite types (tuples/records). This function is the output counterpart to PLy_input_setup_tuple, handling the reverse conversion direction from Python to PostgreSQL.

The function performs several key operations:
1. Validates that the structure is set up for composite type output conversion (PLyObject_ToComposite)
2. For anonymous record types (RECORDOID with negative typmod), stores a reference to the tuple descriptor
3. Allocates or reallocates the attributes array if the number of columns has changed
4. Sets up individual column conversion information for each non-dropped attribute

Like its input counterpart, the function is designed to handle re-initialization efficiently, only updating conversion information for columns whose type or type modifier has changed since the last setup.

## Parameters / Member Variables
- `arg`: PLyObToDatum structure that will be configured for Python-to-tuple conversion
- `desc`: TupleDesc describing the structure and types of the target composite types columns  
- `proc`: PLyProcedure context containing procedure-specific conversion settings

## Dependencies
- Functions called/Symbols referenced:
  - [PLyObject_ToComposite](PLyObject_ToComposite.md) (validation)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [PLy_output_setup_func](PLy_output_setup_func.md)
  - [pfree](../p/pfree.md)
  - TupleDescAttr
- Called from (representative examples):
  - [PLy_exec_trigger](PLy_exec_trigger.md)
  - [PLy_output_setup_record](PLy_output_setup_record.md)
  - [PLyObject_ToComposite](PLyObject_ToComposite.md)

## Notes and Other Information
- This function is the output conversion counterpart to PLy_input_setup_tuple
- Specifically designed to handle anonymous record types where tuple descriptors must be passed explicitly
- Only stores tuple descriptor references for anonymous record types (RECORDOID with negative typmod)
- Efficiently handles re-initialization by only updating changed column conversion information
- Memory allocation occurs in the specified memory context (arg->mcxt) for proper cleanup
- Skips dropped columns during setup to avoid unnecessary conversion overhead
- The caller is responsible for ensuring adequate lifespan of the tuple descriptor for anonymous record types
- For named composite or registered record types, the tuple descriptor does not need to be long-lived
- This function works in conjunction with PLyObject_ToComposite to provide complete Python-to-tuple conversion functionality
- Part of the broader PL/Python type conversion system that enables seamless data exchange between Python and PostgreSQL