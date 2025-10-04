# ttdummy

## Location
[src/test/regress/regress.c:275-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L275-L463)

## Overview
This function implements a PostgreSQL trigger that manages temporal database functionality by automatically handling start and stop date columns to maintain historical record versioning.

## Definition
```c
Datum ttdummy(PG_FUNCTION_ARGS)
```

## Detailed Description
ttdummy is a sophisticated PostgreSQL trigger function that implements temporal table functionality, allowing tables to maintain historical versions of rows by managing start and stop date columns. The function validates that it's called as a BEFORE ROW trigger (not for INSERT operations), then processes UPDATE and DELETE operations by creating new historical records. For UPDATE operations, it ensures the temporal columns cannot be manually modified and creates a new row with updated temporal values. For DELETE operations, it sets the stop date to mark the record as ended. The function uses SPI (Server Programming Interface) to insert historical records and maintains consistency through proper validation and error handling.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to:
  - `fcinfo->context`: Contains TriggerData with trigger-specific information
  - TriggerData fields used:
    - `tg_trigtuple`: The old tuple being modified
    - `tg_newtuple`: The new tuple (for UPDATE operations)
    - `tg_relation`: The relation being modified
    - `tg_trigger`: Trigger definition with arguments
    - `tg_event`: Event type information
- Trigger arguments (expected 2):
  - `args[0]`: Name of start date column (must be integer type)
  - `args[1]`: Name of stop date column (must be integer type)

## Dependencies
- Functions called/Symbols referenced:
  - CALLED_AS_TRIGGER (validates trigger context)
  - TRIGGER_FIRED_FOR_ROW, TRIGGER_FIRED_BEFORE, TRIGGER_FIRED_BY_INSERT, TRIGGER_FIRED_BY_UPDATE (trigger event validation)
  - [SPI_getrelname](../S/SPI_getrelname.md), SPI_fnumber, SPI_gettypeid, SPI_getbinval (SPI data access functions)
  - [SPI_connect](../S/SPI_connect.md), SPI_prepare, SPI_keepplan, SPI_execp, SPI_modifytuple, SPI_finish (SPI execution functions)
  - DirectFunctionCall1, nextval (sequence value generation)
  - TTDUMMY_INFINITY (constant for infinite date values)
  - [palloc](../p/palloc.md), pfree (PostgreSQL memory management)
  - elog, ereport (error reporting)
- Called from (representative examples):
  - Referenced by TTDUMMY_INFINITY constant

## Notes and Other Information
- This function is part of PostgreSQL's regression test suite demonstrating temporal table implementation
- Requires exactly 2 arguments specifying the start and stop date column names
- Only works with BEFORE ROW triggers and prohibits INSERT operations
- Maintains historical integrity by preventing manual modification of temporal columns
- Uses a sequence ('ttdummy_seq') to generate timestamp values for temporal columns
- Creates audit trail by inserting historical records before modifying current data
- The global variable 'ttoff' can disable the temporal functionality when set
- Implements sophisticated validation to ensure data consistency and proper temporal semantics
- Located in src/test/regress/regress.c, primarily used for testing temporal database patterns
- Demonstrates advanced PostgreSQL trigger programming including SPI usage and plan caching

## Simplified Source

```c
Datum ttdummy(PG_FUNCTION_ARGS) {
    // Validate trigger context and event type
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    if (!CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "ttdummy: not fired by trigger manager");
    if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event) ||
        !TRIGGER_FIRED_BEFORE(trigdata->tg_event) ||
        TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
        elog(ERROR, "ttdummy: invalid trigger type");

    // Get tuple and relation info
    HeapTuple trigtuple = trigdata->tg_trigtuple;
    HeapTuple newtuple = TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) ?
                         trigdata->tg_newtuple : NULL;
    Relation rel = trigdata->tg_relation;
    char *relname = SPI_getrelname(rel);

    // Early exit if temporal tracking is disabled
    if (ttoff) {
        pfree(relname);
        return PointerGetDatum((newtuple != NULL) ? newtuple : trigtuple);
    }

    // Validate trigger arguments (start/stop column names)
    Trigger *trigger = trigdata->tg_trigger;
    if (trigger->tgnargs != 2)
        elog(ERROR, "ttdummy: invalid number of arguments");

    // Find and validate start/stop date columns
    int attnum[2];
    TupleDesc tupdesc = rel->rd_att;
    for (int i = 0; i < 2; i++) {
        attnum[i] = SPI_fnumber(tupdesc, trigger->tgargs[i]);
        if (attnum[i] <= 0 || SPI_gettypeid(tupdesc, attnum[i]) != INT4OID)
            elog(ERROR, "ttdummy: invalid temporal column");
    }

    // Get old temporal values and validate
    bool isnull;
    Datum oldon = SPI_getbinval(trigtuple, tupdesc, attnum[0], &isnull);
    Datum oldoff = SPI_getbinval(trigtuple, tupdesc, attnum[1], &isnull);

    // For UPDATE: prevent manual changes to temporal columns
    if (newtuple != NULL) {
        Datum newon = SPI_getbinval(newtuple, tupdesc, attnum[0], &isnull);
        Datum newoff = SPI_getbinval(newtuple, tupdesc, attnum[1], &isnull);

        if (oldon != newon || oldoff != newoff)
            elog(ERROR, "ttdummy: cannot change temporal columns manually");

        // Skip if already ended
        if (newoff != TTDUMMY_INFINITY) {
            pfree(relname);
            return PointerGetDatum(NULL);
        }
    } else if (oldoff != TTDUMMY_INFINITY) {
        // DELETE: skip if already ended
        pfree(relname);
        return PointerGetDatum(NULL);
    }

    // Generate new timestamp from sequence
    Datum newoff = DirectFunctionCall1(nextval, CStringGetTextDatum("ttdummy_seq"));
    newoff = Int32GetDatum((int32) DatumGetInt64(newoff));

    // Create historical record via SPI
    SPI_connect();

    // Prepare column values for INSERT
    int natts = tupdesc->natts;
    Datum *cvals = (Datum *) palloc(natts * sizeof(Datum));
    char *cnulls = (char *) palloc(natts * sizeof(char));

    for (int i = 0; i < natts; i++) {
        cvals[i] = SPI_getbinval((newtuple != NULL) ? newtuple : trigtuple,
                                tupdesc, i + 1, &isnull);
        cnulls[i] = (isnull) ? 'n' : ' ';
    }

    // Update temporal columns for historical record
    if (newtuple) {
        // UPDATE: new record starts now, old ends at infinity
        cvals[attnum[0] - 1] = newoff;
        cvals[attnum[1] - 1] = TTDUMMY_INFINITY;
    } else {
        // DELETE: end current record
        cvals[attnum[1] - 1] = newoff;
    }

    // Execute INSERT using prepared plan
    if (splan == NULL) {
        // Create and cache INSERT plan (simplified)
        // ... plan preparation logic ...
    }
    SPI_execp(splan, cvals, cnulls, 0);

    // Return modified tuple for UPDATE, original for DELETE
    HeapTuple rettuple = newtuple ?
        SPI_modifytuple(rel, trigtuple, 1, &(attnum[1]), &newoff, NULL) :
        trigtuple;

    SPI_finish();
    pfree(relname);
    return PointerGetDatum(rettuple);
}
```