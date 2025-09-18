# pltcl_trigger_handler

## Location
src/pl/tcl/pltcl.c: 1056 - 1315

## Overview
Handles trigger calls for PL/Tcl, managing trigger context setup, argument conversion, Tcl trigger function execution, and result processing for both row and statement-level triggers.

## Definition
```c
static HeapTuple pltcl_trigger_handler(PG_FUNCTION_ARGS, pltcl_call_state *call_state, bool pltrusted)
```

## Detailed Description
`pltcl_trigger_handler` is the specialized trigger execution handler for PL/Tcl that processes database trigger calls. It extracts trigger context information (trigger name, relation details, event type, timing, level), converts PostgreSQL trigger data to Tcl format, executes the Tcl trigger function, and processes the return value to determine trigger behavior.

The function handles both row-level and statement-level triggers across all trigger events (INSERT, UPDATE, DELETE, TRUNCATE) and timings (BEFORE, AFTER, INSTEAD OF). It provides comprehensive trigger context to the Tcl function including trigger metadata, relation information, OLD/NEW tuple data for row triggers, and user-defined trigger arguments. The function supports trigger transition tables and manages proper memory context and exception handling throughout execution.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing trigger context
- `call_state`: Pointer to pltcl_call_state structure tracking execution state and trigger data
- `pltrusted`: Boolean flag indicating whether to operate in trusted (true) or untrusted (false) mode

## Dependencies
- Functions called/Symbols referenced:
  - SPI_connect/SPI_finish (SPI interface management)
  - [SPI_register_trigger_data](../S/SPI_register_trigger_data.md) (transition table registration)
  - compile_pltcl_function (function compilation/lookup)
  - pltcl_build_tuple_argument (tuple to Tcl conversion)
  - pltcl_build_tuple_result (Tcl to tuple conversion)
  - TriggerData structure and related macros (trigger context)
  - DirectFunctionCall1/oidout (OID conversion)
  - SPI_getrelname/SPI_getnspname (relation metadata)
  - TRIGGER_FIRED_* macros (trigger event detection)
  - throw_tcl_error (error handling)
  - [utf_e2u](../u/utf_e2u.md)/utf_u2e (encoding conversion)
  - Tcl library functions (Tcl_EvalObjEx, Tcl_ListObjAppendElement, etc.)
- Called from (representative examples):
  - [pltcl_handler](pltcl_handler.md) (main dispatcher)

## Notes and Other Information
- This is a static function, not directly accessible outside the PL/Tcl module
- Returns HeapTuple (modified tuple for row triggers) or NULL (to skip trigger action)
- Supports magic return values "OK" (return original tuple) and "SKIP" (return NULL)
- Handles stored generated columns properly (excludes them from BEFORE trigger NEW rows)
- Provides comprehensive trigger context to Tcl functions including:
  - TG_name (trigger name)
  - TG_relid (relation OID)
  - TG_table_name (table name)
  - TG_table_schema (schema name)
  - TG_relatts (attribute names)
  - TG_when (BEFORE/AFTER/INSTEAD OF)
  - TG_level (ROW/STATEMENT)
  - TG_op (INSERT/UPDATE/DELETE/TRUNCATE)
  - NEW and OLD tuple data (for row triggers)
  - User-defined trigger arguments
- Supports both row-level and statement-level triggers with appropriate context
- Implements proper exception handling with resource cleanup
- Manages transition table visibility for complex trigger scenarios