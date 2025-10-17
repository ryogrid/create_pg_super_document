# each_worker_jsonb

## Location
[src/backend/utils/adt/jsonfuncs.c:1972-2055](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1972-L2055)

## Overview
The each_worker_jsonb function is the core implementation for expanding JSONB objects into key-value pairs, supporting both native JSONB and text output formats.

## Definition
```c
static Datum each_worker_jsonb(FunctionCallInfo fcinfo, const char *funcname, bool as_text)
```

## Detailed Description
This function implements the core logic for JSONB object expansion operations. It takes a JSONB object and iterates through its key-value pairs, returning them as a set of tuples. The function validates that the input is a JSONB object (not an array or scalar), then uses the JsonbIterator interface to traverse the object structure. For each key-value pair, it creates a tuple containing the key as text and the value in either JSONB format or text format depending on the as_text parameter. The function uses a temporary memory context for efficient memory management during iteration.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing arguments and result context
- `funcname`: String name of the calling function (used for error messages)
- `as_text`: Boolean flag indicating whether values should be returned as text (true) or native JSONB (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P (get JSONB argument)
  - JB_ROOT_IS_OBJECT (validate object type)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initialize set-returning function)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md), JsonbIteratorNext (JSONB iteration)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (key conversion)
  - [JsonbValueAsText](../J/JsonbValueAsText.md), JsonbValueToJsonb (value conversion)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md) (result storage)
  - AllocSetContextCreate, MemoryContextDelete (memory management)
- Called from (representative examples):
  - [jsonb_each](../j/jsonb_each.md) (with as_text=false)
  - [jsonb_each_text](../j/jsonb_each_text.md) (with as_text=true)

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:1972-2055
- Static function (internal implementation detail)
- Uses MaterializedSRF pattern for set-returning functions
- Implements proper memory management with temporary contexts
- Handles JSON null values appropriately in text mode (converts to SQL NULL)
- Validates input type and reports meaningful error messages
- Core component of PostgreSQL's JSONB expansion functionality

## Simplified Source

```c
static Datum
each_worker_jsonb(FunctionCallInfo fcinfo, const char *funcname, bool as_text)
{
    Jsonb *jsonb_input = PG_GETARG_JSONB_P(0);

    // Validate input is a JSONB object
    if (!JB_ROOT_IS_OBJECT(jsonb_input))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("cannot call %s on a non-object", funcname)));

    // Initialize set-returning function
    ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);

    // Create temporary memory context for tuple processing
    MemoryContext tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                                  "jsonb_each temporary cxt",
                                                  ALLOCSET_DEFAULT_SIZES);

    // Initialize JSONB iterator
    JsonbIterator *it = JsonbIteratorInit(&jsonb_input->root);
    JsonbValue v;
    bool skipNested = false;

    // Iterate through JSONB object key-value pairs
    JsonbIteratorToken token;
    while ((token = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
    {
        skipNested = true;

        if (token == WJB_KEY)
        {
            // Process key-value pair
            Datum values[2];
            bool nulls[2] = {false, false};

            MemoryContext old_cxt = MemoryContextSwitchTo(tmp_cxt);

            // Convert key to text
            text *key = cstring_to_text_with_len(v.val.string.val, v.val.string.len);
            values[0] = PointerGetDatum(key);

            // Get the corresponding value
            token = JsonbIteratorNext(&it, &v, skipNested);

            // Convert value based on output format
            if (as_text)
            {
                if (v.type == jbvNull)
                {
                    nulls[1] = true;
                    values[1] = (Datum) NULL;
                }
                else
                    values[1] = PointerGetDatum(JsonbValueAsText(&v));
            }
            else
            {
                // Return as JSONB
                Jsonb *val = JsonbValueToJsonb(&v);
                values[1] = PointerGetDatum(val);
            }

            // Store the tuple result
            tuplestore_putvalues(rsi->setResult, rsi->setDesc, values, nulls);

            // Clean up temporary memory
            MemoryContextSwitchTo(old_cxt);
            MemoryContextReset(tmp_cxt);
        }
    }

    MemoryContextDelete(tmp_cxt);
    PG_RETURN_NULL();
}
```