# plperl_trigger_build_args

## Location
[src/pl/plperl/plperl.c:1631-1743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1631-L1743)

## Overview
Constructs a comprehensive Perl hash reference containing all trigger-related information and arguments for PL/Perl trigger functions.

## Definition
static SV *plperl_trigger_build_args(FunctionCallInfo fcinfo)

## Detailed Description
This function creates a complete data structure that PL/Perl trigger functions receive as their argument. It extracts information from the PostgreSQL trigger context and organizes it into a Perl hash with standardized keys. The function handles all trigger types (INSERT, UPDATE, DELETE, TRUNCATE) and timing (BEFORE, AFTER, INSTEAD OF), providing access to both old and new tuple data where applicable. Special handling is implemented for generated columns in BEFORE triggers, where computed columns are not yet available in the NEW row. The function also converts trigger arguments, relation metadata, and timing information into Perl-accessible formats.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing the trigger context data

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall1
  - [oidout](../o/oidout.md)
  - [DatumGetCString](../D/DatumGetCString.md)
  - [cstr2sv](../c/cstr2sv.md)
  - [hv_store_string](../h/hv_store_string.md)
  - [plperl_hash_from_tuple](plperl_hash_from_tuple.md)
  - newRV_noinc
  - [SPI_getrelname](../S/SPI_getrelname.md)
  - [SPI_getnspname](../S/SPI_getnspname.md)
  - TRIGGER_FIRED_BY_INSERT/UPDATE/DELETE/TRUNCATE
  - TRIGGER_FIRED_FOR_ROW/STATEMENT
  - TRIGGER_FIRED_BEFORE/AFTER/INSTEAD
- Called from (representative examples):
  - [plperl_trigger_handler](plperl_trigger_handler.md)

## Notes and Other Information
- Returns a hash reference with keys: name, relid, event, argc, args, relname, table_name, table_schema, when, level, old, new
- Handles generated columns correctly by excluding them from NEW row in BEFORE triggers
- Pre-grows hash to 12 elements for performance optimization
- Provides both "relname" and "table_name" keys for compatibility
- Converts trigger arguments array to Perl array reference when present
- Event types: INSERT, UPDATE, DELETE, TRUNCATE, or UNKNOWN
- Timing types: BEFORE, AFTER, INSTEAD OF, or UNKNOWN  
- Level types: ROW, STATEMENT, or UNKNOWN
- Old/new tuple data only included for row-level triggers where applicable

## Simplified Source

```c
static SV *plperl_trigger_build_args(FunctionCallInfo fcinfo)
{
    TriggerData *tdata;
    TupleDesc tupdesc;
    int i;
    char *level, *event, *relid, *when;
    HV *hv;

    // Create hash and pre-grow for performance
    hv = newHV();
    hv_ksplit(hv, 12);

    tdata = (TriggerData *) fcinfo->context;
    tupdesc = tdata->tg_relation->rd_att;

    // Get relation ID as string
    relid = DatumGetCString(DirectFunctionCall1(oidout,
                                               ObjectIdGetDatum(tdata->tg_relation->rd_id)));

    // Basic trigger information
    hv_store_string(hv, "name", cstr2sv(tdata->tg_trigger->tgname));
    hv_store_string(hv, "relid", cstr2sv(relid));

    // Determine event type and handle tuple data
    if (TRIGGER_FIRED_BY_INSERT(tdata->tg_event))
    {
        event = "INSERT";
        if (TRIGGER_FIRED_FOR_ROW(tdata->tg_event))
            hv_store_string(hv, "new", plperl_hash_from_tuple(tdata->tg_trigtuple, tupdesc,
                                                             !TRIGGER_FIRED_BEFORE(tdata->tg_event)));
    }
    else if (TRIGGER_FIRED_BY_DELETE(tdata->tg_event))
    {
        event = "DELETE";
        if (TRIGGER_FIRED_FOR_ROW(tdata->tg_event))
            hv_store_string(hv, "old", plperl_hash_from_tuple(tdata->tg_trigtuple, tupdesc, true));
    }
    else if (TRIGGER_FIRED_BY_UPDATE(tdata->tg_event))
    {
        event = "UPDATE";
        if (TRIGGER_FIRED_FOR_ROW(tdata->tg_event))
        {
            hv_store_string(hv, "old", plperl_hash_from_tuple(tdata->tg_trigtuple, tupdesc, true));
            hv_store_string(hv, "new", plperl_hash_from_tuple(tdata->tg_newtuple, tupdesc,
                                                             !TRIGGER_FIRED_BEFORE(tdata->tg_event)));
        }
    }
    else if (TRIGGER_FIRED_BY_TRUNCATE(tdata->tg_event))
        event = "TRUNCATE";
    else
        event = "UNKNOWN";

    hv_store_string(hv, "event", cstr2sv(event));
    hv_store_string(hv, "argc", newSViv(tdata->tg_trigger->tgnargs));

    // Handle trigger arguments
    if (tdata->tg_trigger->tgnargs > 0)
    {
        AV *av = newAV();
        av_extend(av, tdata->tg_trigger->tgnargs);
        for (i = 0; i < tdata->tg_trigger->tgnargs; i++)
            av_push(av, cstr2sv(tdata->tg_trigger->tgargs[i]));
        hv_store_string(hv, "args", newRV_noinc((SV *) av));
    }

    // Relation metadata
    hv_store_string(hv, "relname", cstr2sv(SPI_getrelname(tdata->tg_relation)));
    hv_store_string(hv, "table_name", cstr2sv(SPI_getrelname(tdata->tg_relation)));
    hv_store_string(hv, "table_schema", cstr2sv(SPI_getnspname(tdata->tg_relation)));

    // Determine timing and level
    when = TRIGGER_FIRED_BEFORE(tdata->tg_event) ? "BEFORE" :
           TRIGGER_FIRED_AFTER(tdata->tg_event) ? "AFTER" :
           TRIGGER_FIRED_INSTEAD(tdata->tg_event) ? "INSTEAD OF" : "UNKNOWN";
    hv_store_string(hv, "when", cstr2sv(when));

    level = TRIGGER_FIRED_FOR_ROW(tdata->tg_event) ? "ROW" :
            TRIGGER_FIRED_FOR_STATEMENT(tdata->tg_event) ? "STATEMENT" : "UNKNOWN";
    hv_store_string(hv, "level", cstr2sv(level));

    return newRV_noinc((SV *) hv);
}
```