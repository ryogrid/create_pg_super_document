# tsvector_update_trigger

## Location
[src/backend/utils/adt/tsvector_op.c:2739-2891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2739-L2891)

## Overview
The core implementation function for PostgreSQL triggers that automatically update tsvector columns when text columns are modified, supporting both named configurations and column-based configurations.

## Definition

```c
static Datum
tsvector_update_trigger(PG_FUNCTION_ARGS, bool config_column)
```
## Detailed Description
The  function is the main implementation for automatic tsvector maintenance triggers in PostgreSQL. This static function handles the complete workflow of parsing text from specified columns, applying text search configuration, and updating the target tsvector column. It supports two modes of operation based on the  parameter: using a literal configuration name or referencing a regconfig column.

The function performs extensive validation of trigger arguments, column types, and trigger context. It processes text from multiple source columns, parses them using the specified text search configuration, and generates a combined tsvector. The function is designed to work with BEFORE INSERT or BEFORE UPDATE row-level triggers and includes optimization logic to avoid unnecessary updates when text columns haven't changed.

The function handles memory management carefully, using PostgreSQL's memory context system and properly cleaning up allocated resources. It also integrates with PostgreSQL's trigger infrastructure, including proper handling of trigger data structures and column update tracking.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing trigger information
- : Boolean flag indicating configuration source mode:
  - : Configuration specified by name (literal string)
  - : Configuration specified by regconfig column ID

## Dependencies
- Functions called/Symbols referenced:
  -  - Verify function called in trigger context
  -  - Check if trigger fired for row-level operation
  -  - Verify trigger is BEFORE trigger
  -  - Check if triggered by INSERT
  -  - Check if triggered by UPDATE
  -  - Get column number by name
  -  - Get column data type OID
  -  - Check type compatibility
  -  - Extract column value from tuple
  -  - Convert Datum to OID
  -  - Parse qualified configuration name
  -  - Look up text search configuration OID
  -  - Check if column was updated
  -  - Convert Datum to text
  -  - Parse text using text search configuration
  -  - Create tsvector from parsed text
  -  - Convert tsvector to Datum
  -  - Modify tuple columns
- Called from (representative examples):
  -  - Called with config_column=false
  -  - Called with config_column=true

## Notes and Other Information
- This is a static function, not directly callable from SQL - accessed through wrapper functions
- Must be called in BEFORE INSERT or BEFORE UPDATE trigger context, not AFTER triggers
- Requires minimum of 3 arguments: tsvector_column, config_source, text_column1
- Supports multiple text source columns that are concatenated during processing
- Includes optimization to skip tsvector updates when no relevant text columns changed
- Configuration names must be schema-qualified when using literal names for security
- Handles NULL values gracefully - NULL text columns are skipped, but NULL config columns cause errors
- Memory allocation uses palloc and is automatically cleaned up by PostgreSQL's memory context system
- Integrates with PostgreSQL's column update tracking system for efficient UPDATE operations
- Part of PostgreSQL's comprehensive full-text search infrastructure
- Returns modified HeapTuple that will be used for the actual database operation

## Simplified Source

```c
static Datum tsvector_update_trigger(PG_FUNCTION_ARGS, bool config_column) {
    TriggerData *trigdata;
    Trigger *trigger;
    Relation rel;
    HeapTuple rettuple = NULL;
    int tsvector_attr_num;
    ParsedText prs;
    Datum datum;
    bool isnull;
    text *txt;
    Oid cfgId;
    bool update_needed;

    // Validate trigger context
    if (!CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "tsvector_update_trigger: not fired by trigger manager");

    trigdata = (TriggerData *) fcinfo->context;
    if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
        elog(ERROR, "must be fired for row");
    if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
        elog(ERROR, "must be fired BEFORE event");

    // Determine which tuple to process
    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)) {
        rettuple = trigdata->tg_trigtuple;
        update_needed = true;
    } else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) {
        rettuple = trigdata->tg_newtuple;
        update_needed = false; // computed below
    } else {
        elog(ERROR, "must be fired for INSERT or UPDATE");
    }

    trigger = trigdata->tg_trigger;
    rel = trigdata->tg_relation;

    // Validate arguments: tsvector_field, config, text_field1, ...
    if (trigger->tgnargs < 3)
        elog(ERROR, "arguments must be tsvector_field, ts_config, text_field1, ...");

    // Find target tsvector column and validate type
    tsvector_attr_num = SPI_fnumber(rel->rd_att, trigger->tgargs[0]);
    if (tsvector_attr_num == SPI_ERROR_NOATTRIBUTE)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg("tsvector column \"%s\" does not exist", trigger->tgargs[0])));

    // Get text search configuration
    if (config_column) {
        // Configuration from column value
        int config_attr_num = SPI_fnumber(rel->rd_att, trigger->tgargs[1]);
        datum = SPI_getbinval(rettuple, rel->rd_att, config_attr_num, &isnull);
        if (isnull)
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("configuration column must not be null")));
        cfgId = DatumGetObjectId(datum);
    } else {
        // Configuration from literal name
        List *names = stringToQualifiedNameList(trigger->tgargs[1], NULL);
        if (list_length(names) < 2)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("text search configuration name must be schema-qualified")));
        cfgId = get_ts_config_oid(names, false);
    }

    // Initialize parser for text processing
    prs.lenwords = 32;
    prs.curwords = 0;
    prs.pos = 0;
    prs.words = (ParsedWord *) palloc(sizeof(ParsedWord) * prs.lenwords);

    // Process all text columns (args[2] onwards)
    for (int i = 2; i < trigger->tgnargs; i++) {
        int numattr = SPI_fnumber(rel->rd_att, trigger->tgargs[i]);

        // Check if this column was updated (for UPDATE triggers)
        if (bms_is_member(numattr - FirstLowInvalidHeapAttributeNumber,
                         trigdata->tg_updatedcols))
            update_needed = true;

        // Extract and parse text from column
        datum = SPI_getbinval(rettuple, rel->rd_att, numattr, &isnull);
        if (isnull)
            continue;

        txt = DatumGetTextPP(datum);
        parsetext(cfgId, &prs, VARDATA_ANY(txt), VARSIZE_ANY_EXHDR(txt));

        if (txt != (text *) DatumGetPointer(datum))
            pfree(txt);
    }

    // Update tsvector column if needed
    if (update_needed) {
        datum = TSVectorGetDatum(make_tsvector(&prs));
        isnull = false;

        rettuple = heap_modify_tuple_by_cols(rettuple, rel->rd_att,
                                           1, &tsvector_attr_num,
                                           &datum, &isnull);
        pfree(DatumGetPointer(datum));
    }

    return PointerGetDatum(rettuple);
}
```