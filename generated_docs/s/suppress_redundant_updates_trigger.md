# suppress_redundant_updates_trigger

## Location
[src/backend/utils/adt/trigfuncs.c:28-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/trigfuncs.c#L28-L84)

## Overview
A trigger function designed to optimize database performance by preventing redundant UPDATE operations when the OLD and NEW row data are identical.

## Definition

```c
Datum
suppress_redundant_updates_trigger(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL trigger function that performs a byte-level comparison between the old and new tuple data in an UPDATE operation. If the data is identical, it suppresses the update by returning NULL, which prevents unnecessary write operations, WAL logging, and potential cascading effects.

The function performs comprehensive validation to ensure it's called in the correct context:
- Must be invoked as a trigger (not a regular function)
- Must be fired by an UPDATE operation (not INSERT or DELETE)
- Must be a BEFORE trigger (not AFTER or INSTEAD OF)
- Must be a row-level trigger (not statement-level)

The comparison algorithm checks multiple aspects of the tuple structure:
- Overall tuple length ()
- Header offset () indicating where actual data begins
- Number of attributes using 
- Info mask flags (excluding transaction-related bits)
- Byte-by-byte comparison of the actual tuple data payload

If all these elements match exactly, the function returns NULL to suppress the update; otherwise, it returns the new tuple to allow the update to proceed.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides access to:
  - : Contains  structure with trigger context information
  - : The proposed new tuple data
  - : The existing old tuple data
  - : Bitmask indicating trigger firing conditions

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to verify function is called as a trigger
  - : Macro to check if triggered by UPDATE
  - : Macro to verify it's a BEFORE trigger
  - : Macro to ensure row-level trigger
  - : Function to get number of attributes in tuple
  - : Error reporting function
  - : Converts pointer to PostgreSQL Datum
  - : Structure containing trigger execution context
  - : Structure representing tuple header information
  - : Bitmask for transaction-related flags
  - : Constant defining tuple header size

- Called from (representative examples):
  - No direct references found in the codebase (typically used via CREATE TRIGGER statements)

## Notes and Other Information
- This function is commonly used to optimize tables with frequent updates where many operations don't actually change data values
- The function is particularly useful for tables updated by ORMs or applications that issue UPDATE statements regardless of whether data has changed  
- Performance benefit comes from avoiding WAL logging, index updates, and trigger cascades for no-op updates
- The byte-level comparison is very thorough but may not catch semantically equivalent data stored in different binary formats
- Located in 
- Returns NULL to suppress update, or the new tuple pointer to allow update
- Must be created as a BEFORE UPDATE FOR EACH ROW trigger to function correctly
- Error messages use  for improper usage

## Simplified Source

```c
// Simplified version of suppress_redundant_updates_trigger
Datum suppress_redundant_updates_trigger(PG_FUNCTION_ARGS) {
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    HeapTuple newtuple, oldtuple, result;
    HeapTupleHeader newheader, oldheader;

    // Step 1: Validate trigger calling context
    if (!CALLED_AS_TRIGGER(fcinfo)) {
        ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                 errmsg("suppress_redundant_updates_trigger: must be called as trigger")));
    }

    if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) {
        ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                 errmsg("suppress_redundant_updates_trigger: must be called on update")));
    }

    if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event)) {
        ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                 errmsg("suppress_redundant_updates_trigger: must be called before update")));
    }

    if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event)) {
        ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                 errmsg("suppress_redundant_updates_trigger: must be called for each row")));
    }

    // Step 2: Extract old and new tuple data
    newtuple = trigdata->tg_newtuple;
    oldtuple = trigdata->tg_trigtuple;
    result = newtuple;  // Default: allow the update

    newheader = newtuple->t_data;
    oldheader = oldtuple->t_data;

    // Step 3: Compare tuple structure and data
    bool tuplesIdentical = (
        // Same overall size
        newtuple->t_len == oldtuple->t_len &&

        // Same header structure
        newheader->t_hoff == oldheader->t_hoff &&

        // Same number of attributes
        HeapTupleHeaderGetNatts(newheader) == HeapTupleHeaderGetNatts(oldheader) &&

        // Same info flags (excluding transaction bits)
        (newheader->t_infomask & ~HEAP_XACT_MASK) ==
        (oldheader->t_infomask & ~HEAP_XACT_MASK) &&

        // Same actual data content
        memcmp(((char *) newheader) + SizeofHeapTupleHeader,
               ((char *) oldheader) + SizeofHeapTupleHeader,
               newtuple->t_len - SizeofHeapTupleHeader) == 0
    );

    // Step 4: Decide whether to suppress or allow the update
    if (tuplesIdentical) {
        result = NULL;  // Suppress redundant update
    }

    return PointerGetDatum(result);
}
```

Key simplifications made:
- Added clear step-by-step comments explaining the validation and comparison process
- Consolidated the multi-condition comparison into a single boolean expression for clarity
- Made the four validation checks more explicit and organized
- Simplified variable organization and eliminated intermediate assignments
- Added comments explaining what each comparison checks (size, structure, attributes, flags, data)
- Made the final decision logic more explicit (suppress vs. allow)
- Focused on the core algorithm: validate context → extract data → compare → decide
- Preserved all essential redundancy detection functionality