# plsample_trigger_handler

## Location
src/test/modules/plsample/plsample.c: 205 - 354

## Overview
Handles the execution of trigger functions in the plsample procedural language, demonstrating comprehensive trigger introspection, SPI integration, and trigger event processing.

## Definition
```c
static HeapTuple plsample_trigger_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
`plsample_trigger_handler` is the trigger execution handler for the plsample procedural language. This function provides a complete example implementation showing how procedural language handlers can process PostgreSQL triggers. It demonstrates all aspects of trigger handling including context validation, SPI (Server Programming Interface) integration, trigger metadata extraction, and comprehensive event analysis.

The function performs several key operations:
1. **Context Validation**: Verifies the function was called as a trigger
2. **SPI Integration**: Connects to PostgreSQL's SPI manager for database access
3. **Function Introspection**: Retrieves and displays the trigger function's source code
4. **Trigger Analysis**: Examines trigger metadata including timing (BEFORE/AFTER/INSTEAD OF), events (INSERT/DELETE/UPDATE/TRUNCATE), and level (ROW/STATEMENT)
5. **Argument Processing**: Iterates through and displays all trigger arguments
6. **Exception Handling**: Uses PostgreSQL's PG_TRY/PG_CATCH mechanism for error handling
7. **Resource Cleanup**: Properly disconnects from SPI manager

The function serves as an educational template demonstrating proper trigger handling patterns, SPI usage, and the comprehensive trigger information available to procedural languages.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL macro providing access to:
  - `fcinfo->context`: Cast to TriggerData*, contains all trigger-specific information
  - `trigdata->tg_trigger`: Trigger definition including name and arguments
  - `trigdata->tg_relation`: Relation (table) the trigger is defined on
  - `trigdata->tg_event`: Event information (timing, operation, level)
  - `trigdata->tg_trigtuple`: The tuple that fired the trigger

## Dependencies
- Functions called/Symbols referenced:
  - `CALLED_AS_TRIGGER` (validate trigger context)
  - `SPI_connect`, `SPI_register_trigger_data`, `SPI_finish` (SPI interface)
  - `SPI_getrelname`, `SPI_getnspname` (relation name functions)
  - [SearchSysCache1](../S/SearchSysCache1.md), `ReleaseSysCache` (system catalog access)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md), `DirectFunctionCall1`, `textout` (source extraction)
  - `TRIGGER_FIRED_BY_*` macros (event type detection)
  - `TRIGGER_FIRED_BEFORE/AFTER/INSTEAD` (timing detection)
  - `TRIGGER_FIRED_FOR_ROW/STATEMENT` (level detection)
  - `PG_TRY`, `PG_CATCH`, `PG_RE_THROW`, `PG_END_TRY` (exception handling)
  - `ereport(NOTICE)` (logging and output)
- Called from:
  - [plsample_call_handler](plsample_call_handler.md) (when handling trigger function calls)

## Notes and Other Information
- Located in `src/test/modules/plsample/plsample.c:205-354`
- This is a static function, only accessible within the plsample module
- Returns the trigger tuple (tg_trigtuple) unchanged, demonstrating the basic trigger return pattern
- Provides extensive logging via ereport(NOTICE) for educational purposes, showing:
  - Trigger name and relation information
  - Complete event analysis (operation type, timing, level)
  - All trigger arguments
- Demonstrates proper SPI usage patterns including connection, registration, and cleanup
- Uses PostgreSQL's exception handling framework with proper error propagation
- Includes comprehensive comments explaining where real procedural languages would augment and execute code
- Part of PostgreSQL's test infrastructure, serving as a reference implementation for trigger handlers
- The function handles all trigger types: INSERT, DELETE, UPDATE, and TRUNCATE
- Supports all trigger timings: BEFORE, AFTER, and INSTEAD OF
- Works with both row-level and statement-level triggers
- Properly integrates with PostgreSQL's trigger infrastructure including SPI registration for trigger data access