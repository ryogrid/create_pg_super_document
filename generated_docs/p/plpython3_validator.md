# plpython3_validator

## Location
[src/pl/plpython/plpy_main.c:158-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L158-L190)

## Overview
PostgreSQL function that validates PL/Python function definitions during CREATE FUNCTION operations, ensuring syntax correctness and proper compilation.

## Definition

```c
Datum
plpython3_validator(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the validation handler for PL/Python functions and triggers when they are created or modified. It performs several validation steps including access control checks, function body validation (when enabled), and syntax verification by attempting to compile the Python code. The function retrieves the function definition from the system catalog, determines if it's a trigger function, and validates the Python code by invoking the PL/Python compilation process.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: OID of the function being validated (extracted from arguments)
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (extract OID from function arguments)
  - [CheckFunctionValidatorAccess](../C/CheckFunctionValidatorAccess.md) (security check)
  - PG_RETURN_VOID (return void result)
  - [PLy_initialize](../P/PLy_initialize.md) (initialize PL/Python environment)
  - [SearchSysCache1](../S/SearchSysCache1.md) (lookup function in system cache)
  - HeapTupleIsValid (validate heap tuple)
  - elog (error logging)
  - GETSTRUCT (extract structure from heap tuple)
  - [PLy_procedure_is_trigger](../P/PLy_procedure_is_trigger.md) (check if function is trigger)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release system cache)
  - [PLy_procedure_get](../P/PLy_procedure_get.md) (compile and validate procedure)
- Called from (representative examples):
  - PostgreSQL's function validation system during CREATE FUNCTION

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c at lines 157-188
- Returns Datum type following PostgreSQL function call conventions
- Validation is skipped if check_function_bodies is disabled
- Performs access control checks before proceeding with validation
- Part of PostgreSQL's procedural language validation infrastructure
- Handles both regular functions and trigger functions through PLy_procedure_is_trigger check
- Uses InvalidOid for trigger validation since triggers aren't bound to specific tables during validation