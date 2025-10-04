# plsample_trigger_handler

## Location
[src/test/modules/plsample/plsample.c:205-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/plsample/plsample.c#L205-L354)

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
  - `[SPI_connect](../S/SPI_connect.md)`, `SPI_register_trigger_data`, `SPI_finish` (SPI interface)
  - `[SPI_getrelname](../S/SPI_getrelname.md)`, `SPI_getnspname` (relation name functions)
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
  - [Trigger](../T/Trigger.md) name and relation information
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

## Simplified Source

```c
static HeapTuple plsample_trigger_handler(PG_FUNCTION_ARGS) {
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    char *string;
    volatile HeapTuple rettup;
    HeapTuple pl_tuple;
    Datum ret;
    char *source;
    bool isnull;
    Form_pg_proc pl_struct;
    char *proname;
    int rc PG_USED_FOR_ASSERTS_ONLY;

    // Validate trigger context and connect to SPI
    if (!CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "not called by trigger manager");

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "could not connect to SPI manager");

    rc = SPI_register_trigger_data(trigdata);
    Assert(rc >= 0);

    // Fetch function's pg_proc entry and extract source
    pl_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fcinfo->flinfo->fn_oid));
    if (!HeapTupleIsValid(pl_tuple))
        elog(ERROR, "cache lookup failed for function %u", fcinfo->flinfo->fn_oid);

    pl_struct = (Form_pg_proc) GETSTRUCT(pl_tuple);
    proname = pstrdup(NameStr(pl_struct->proname));
    ret = SysCacheGetAttr(PROCOID, pl_tuple, Anum_pg_proc_prosrc, &isnull);
    if (isnull)
        elog(ERROR, "could not find source text of function \"%s\"", proname);
    source = DatumGetCString(DirectFunctionCall1(textout, ret));
    ereport(NOTICE, (errmsg("source text of function \"%s\": %s", proname, source)));

    ReleaseSysCache(pl_tuple);

    // Process trigger information within exception handling
    PG_TRY();
    {
        ereport(NOTICE, (errmsg("trigger name: %s", trigdata->tg_trigger->tgname)));
        string = SPI_getrelname(trigdata->tg_relation);
        ereport(NOTICE, (errmsg("trigger relation: %s", string)));

        string = SPI_getnspname(trigdata->tg_relation);
        ereport(NOTICE, (errmsg("trigger relation schema: %s", string)));

        // Analyze trigger event type
        if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)) {
            ereport(NOTICE, (errmsg("triggered by INSERT")));
            rettup = trigdata->tg_trigtuple;
        } else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)) {
            ereport(NOTICE, (errmsg("triggered by DELETE")));
            rettup = trigdata->tg_trigtuple;
        } else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) {
            ereport(NOTICE, (errmsg("triggered by UPDATE")));
            rettup = trigdata->tg_trigtuple;
        } else if (TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event)) {
            ereport(NOTICE, (errmsg("triggered by TRUNCATE")));
            rettup = trigdata->tg_trigtuple;
        } else
            elog(ERROR, "unrecognized event: %u", trigdata->tg_event);

        // Analyze trigger timing
        if (TRIGGER_FIRED_BEFORE(trigdata->tg_event))
            ereport(NOTICE, (errmsg("triggered BEFORE")));
        else if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
            ereport(NOTICE, (errmsg("triggered AFTER")));
        else if (TRIGGER_FIRED_INSTEAD(trigdata->tg_event))
            ereport(NOTICE, (errmsg("triggered INSTEAD OF")));
        else
            elog(ERROR, "unrecognized when: %u", trigdata->tg_event);

        // Analyze trigger level
        if (TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
            ereport(NOTICE, (errmsg("triggered per row")));
        else if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
            ereport(NOTICE, (errmsg("triggered per statement")));
        else
            elog(ERROR, "unrecognized level: %u", trigdata->tg_event);

        // Display all trigger arguments
        for (int i = 0; i < trigdata->tg_trigger->tgnargs; i++)
            ereport(NOTICE, (errmsg("trigger arg[%i]: %s", i,
                                   trigdata->tg_trigger->tgargs[i])));
    }
    PG_CATCH();
    {
        // Error cleanup would go here in real implementation
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish() failed");

    return rettup;
}
```