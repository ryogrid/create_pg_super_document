# ExecCallTriggerFunc

## Location
src/backend/commands/trigger.c: 2304 - 2395

## Overview
Executes a trigger function with proper context management, instrumentation, and error handling, returning the tuple result as specified by PostgreSQL's trigger protocol.

## Definition
```c
static HeapTuple ExecCallTriggerFunc(TriggerData *trigdata,
                                     int tgindx,
                                     FmgrInfo *finfo,
                                     Instrumentation *instr,
                                     MemoryContext per_tuple_context)
```

## Detailed Description
This function serves as the core execution engine for trigger functions in PostgreSQL. It handles the complete lifecycle of trigger function invocation, including memory context management, function lookup caching, instrumentation for EXPLAIN ANALYZE, and proper error handling according to the trigger protocol.

The function performs several critical tasks:
1. Validates transition table setup for triggers that should have them
2. Caches function manager information to avoid repeated lookups
3. Switches to per-tuple memory context to ensure proper memory cleanup
4. Invokes the actual trigger function with appropriate context
5. Handles instrumentation for performance analysis
6. Validates the return value according to trigger protocol rules
7. Manages the trigger depth counter for nested trigger detection

Key safety features include protection against uninitialized transition table info and enforcement of the trigger protocol requirement that functions cannot return NULL with the isnull flag set.

## Parameters / Member Variables
- `trigdata`: Complete trigger context information including event type, table data, and transition tables
- `tgindx`: Index of the specific trigger in the function info and instrumentation arrays
- `finfo`: Array of cached FmgrInfo structures for efficient function lookup
- `instr`: Optional instrumentation array for EXPLAIN ANALYZE timing and statistics
- `per_tuple_context`: Memory context for executing the trigger function to ensure proper cleanup

## Dependencies
- Functions called/Symbols referenced:
  - fmgr_info (function manager info setup)
  - InitFunctionCallInfoData (function call preparation)
  - FunctionCallInvoke (actual function execution)
  - pgstat_init_function_usage / pgstat_end_function_usage (statistics)
  - InstrStartNode / InstrStopNode (instrumentation)
  - MemoryContextSwitchTo (memory management)
- Macros used:
  - TRIGGER_FIRED_BY_INSERT/UPDATE/DELETE (event type checking)
  - TRIGGER_FIRED_AFTER (timing checking)
  - LOCAL_FCINFO (function call info declaration)
- Called from (representative examples):
  - ExecBSInsertTriggers (before-statement insert triggers)
  - ExecBRInsertTriggers (before-row insert triggers)
  - ExecIRInsertTriggers (instead-of insert triggers)
  - AfterTriggerExecute (deferred trigger execution)

## Notes and Other Information
- Uses PG_TRY/PG_FINALLY blocks to ensure MyTriggerDepth is properly decremented even on errors
- Enforces trigger protocol by checking that functions don't return NULL with isnull flag set
- Memory allocated during trigger execution is automatically cleaned up per tuple cycle
- Function lookup results are cached in finfo array to improve performance for repeated calls
- Instrumentation data helps with query performance analysis in EXPLAIN ANALYZE
- The function is static, indicating it's an internal implementation detail of the trigger system