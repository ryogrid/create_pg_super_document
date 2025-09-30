# ExecCallTriggerFunc

## Location
[src/backend/commands/trigger.c:2304-2395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2304-L2395)

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
  - [fmgr_info](../f/fmgr_info.md) (function manager info setup)
  - InitFunctionCallInfoData (function call preparation)
  - FunctionCallInvoke (actual function execution)
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md) / pgstat_end_function_usage (statistics)
  - [InstrStartNode](../I/InstrStartNode.md) / InstrStopNode (instrumentation)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)
- Macros used:
  - TRIGGER_FIRED_BY_INSERT/UPDATE/DELETE (event type checking)
  - TRIGGER_FIRED_AFTER (timing checking)
  - LOCAL_FCINFO (function call info declaration)
- Called from (representative examples):
  - [ExecBSInsertTriggers](ExecBSInsertTriggers.md) (before-statement insert triggers)
  - [ExecBRInsertTriggers](ExecBRInsertTriggers.md) (before-row insert triggers)
  - [ExecIRInsertTriggers](ExecIRInsertTriggers.md) (instead-of insert triggers)
  - [AfterTriggerExecute](../A/AfterTriggerExecute.md) (deferred trigger execution)

## Notes and Other Information
- Uses PG_TRY/PG_FINALLY blocks to ensure MyTriggerDepth is properly decremented even on errors
- Enforces trigger protocol by checking that functions don't return NULL with isnull flag set
- Memory allocated during trigger execution is automatically cleaned up per tuple cycle
- Function lookup results are cached in finfo array to improve performance for repeated calls
- [Instrumentation](../I/Instrumentation.md) data helps with query performance analysis in EXPLAIN ANALYZE
- The function is static, indicating it's an internal implementation detail of the trigger system

## Simplified Source

```c
static HeapTuple ExecCallTriggerFunc(TriggerData *trigdata, int tgindx,
                                     FmgrInfo *finfo, Instrumentation *instr,
                                     MemoryContext per_tuple_context) {
    LOCAL_FCINFO(fcinfo, 0);
    PgStat_FunctionCallUsage fcusage;
    Datum result;
    MemoryContext oldContext;

    // Validate transition table setup
    Assert(/* complex trigger validation logic */);

    finfo += tgindx;

    // Cache function lookup if needed
    if (finfo->fn_oid == InvalidOid)
        fmgr_info(trigdata->tg_trigger->tgfoid, finfo);

    // Start instrumentation if enabled
    if (instr)
        InstrStartNode(instr + tgindx);

    // Switch to per-tuple memory context
    oldContext = MemoryContextSwitchTo(per_tuple_context);

    // Initialize function call and statistics
    InitFunctionCallInfoData(*fcinfo, finfo, 0, InvalidOid,
                             (Node *) trigdata, NULL);
    pgstat_init_function_usage(fcinfo, &fcusage);

    // Call trigger function with depth tracking
    MyTriggerDepth++;
    PG_TRY();
    {
        result = FunctionCallInvoke(fcinfo);
    }
    PG_FINALLY();
    {
        MyTriggerDepth--;
    }
    PG_END_TRY();

    pgstat_end_function_usage(&fcusage, true);
    MemoryContextSwitchTo(oldContext);

    // Validate trigger protocol - no NULL with isnull flag
    if (fcinfo->isnull)
        ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                        errmsg("trigger function returned null value")));

    // Stop instrumentation
    if (instr)
        InstrStopNode(instr + tgindx, 1);

    return (HeapTuple) DatumGetPointer(result);
}
```