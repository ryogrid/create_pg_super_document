# PLy_procedure_create

## Location
src/pl/plpython/plpy_procedure.c: 133 - 351

## Overview
Creates a new PLyProcedure structure by parsing function metadata, setting up input/output conversion functions, and compiling the Python source code into a callable procedure object.

## Definition
```c
static PLyProcedure *PLy_procedure_create(HeapTuple procTup, Oid fn_oid, bool is_trigger)
```

## Detailed Description
This comprehensive function constructs a complete PLyProcedure object from PostgreSQL system catalog information. It generates a unique Python function name, creates a dedicated memory context, extracts function metadata (name, arguments, return type), validates type compatibility, sets up input/output conversion functions for all parameters and return values, retrieves the function source code, and finally compiles it using PLy_procedure_compile. The function handles both regular functions and trigger functions with appropriate type checking and setup.

## Parameters / Member Variables
- `procTup`: HeapTuple containing the pg_proc system catalog entry for the function
- `fn_oid`: OID of the function being created
- `is_trigger`: Boolean flag indicating whether this is a trigger function

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (memory context creation)
  - MemoryContextSwitchTo/MemoryContextSetIdentifier (memory management)
  - SearchSysCache1/SysCacheGetAttr/ReleaseSysCache (system catalog access)
  - get_func_arg_info (argument information extraction)
  - PLy_output_setup_func/PLy_input_setup_func (I/O function setup)
  - TextDatumGetCString (source code extraction)
  - PLy_procedure_compile (Python compilation)
  - PLy_procedure_delete (cleanup on error)
  - PG_TRY/PG_CATCH/PG_END_TRY (exception handling)
- Called from (representative examples):
  - PLy_procedure_get (cache miss or validation failure scenarios)

## Notes and Other Information
- Creates a dedicated memory context for each procedure to ensure proper cleanup
- Generates Python-safe procedure names by replacing invalid characters with underscores
- Validates argument and return types, rejecting most pseudotypes except void, record, and trigger types
- Handles both regular functions and trigger functions with different setup requirements
- Sets up complete type conversion infrastructure for all input parameters and return values
- Uses exception-safe patterns to ensure cleanup on compilation errors
- The procedure name format is '__plpython_procedure_[original_name]_[oid]' for uniqueness
- Critical for transforming PostgreSQL function definitions into executable Python procedures