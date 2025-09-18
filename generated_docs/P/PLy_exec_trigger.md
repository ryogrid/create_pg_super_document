# PLy_exec_trigger

## Location
[src/pl/plpython/plpy_exec.c:321-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L321-L434)

## Overview
PLy_exec_trigger is the execution handler for PL/Python trigger functions, managing trigger event processing with support for tuple modification, action control, and proper type conversion between PostgreSQL and Python objects.

## Definition


## Detailed Description
This function serves as the core execution handler for PL/Python trigger functions. It handles the complete lifecycle of trigger execution including:

1. **Type System Setup**: Dynamically sets up input/output conversion functions based on the relation's tuple descriptor, handling cases where the relation schema might have changed since the trigger was last called
2. **Trigger Context Management**: Registers trigger data with SPI and builds appropriate trigger arguments for the Python function
3. **Return Value Processing**: Interprets the Python function's return value to control trigger behavior:
   -  or : Accept the tuple as-is
   - : Skip the triggering action
   - : Use the modified tuple (only valid for INSERT/UPDATE triggers)
4. **Tuple Modification**: Handles tuple modification for INSERT and UPDATE triggers when the Python function returns 
5. **Error Handling**: Provides comprehensive validation of return values and proper cleanup

The function expects the Python trigger function to return either None (indicating the tuple is acceptable and unmodified) or a string value indicating the desired action.

## Parameters / Member Variables
- : FunctionCallInfo structure containing the trigger call context and arguments
- : PLyProcedure structure containing the compiled Python trigger procedure information

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_global_args_push](PLy_global_args_push.md)/PLy_global_args_pop
  - [PLy_output_setup_func](PLy_output_setup_func.md)/PLy_input_setup_func
  - [PLy_output_setup_tuple](PLy_output_setup_tuple.md)/PLy_input_setup_tuple
  - [PLy_trigger_build_args](PLy_trigger_build_args.md)
  - [PLy_procedure_call](PLy_procedure_call.md)
  - [PLy_modify_tuple](PLy_modify_tuple.md)
  - [SPI_register_trigger_data](../S/SPI_register_trigger_data.md)/SPI_finish
  - CALLED_AS_TRIGGER, TRIGGER_FIRED_BY_* macros
- Called from (representative examples):
  - [plpython3_call_handler](../p/plpython3_call_handler.md) (main trigger handler)

## Notes and Other Information
- Supports all trigger timing (BEFORE/AFTER) and events (INSERT/UPDATE/DELETE)
- Validates return values strictly: None, "OK", "SKIP", or "MODIFY" only
- "MODIFY" return value is ignored (with warning) for DELETE triggers
- Dynamically adapts to relation schema changes by re-setting up type conversion
- Uses PG_FINALLY block to ensure proper cleanup of Python objects and argument stack
- Integrates with PostgreSQL's SPI system for database access within triggers
- File location: src/pl/plpython/plpy_exec.c:321-434