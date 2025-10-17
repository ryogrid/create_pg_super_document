# elements_worker_jsonb

## Location
[src/backend/utils/adt/jsonfuncs.c:2218-2293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2218-L2293)

## Overview
A static worker function that extracts array elements from a JSONB value and returns them as a set of rows, either as JSONB objects or as text depending on the specified mode.

## Definition

```c
static Datum
elements_worker_jsonb(FunctionCallInfo fcinfo, const char *funcname,
					  bool as_text)
```
## Detailed Description
This function serves as the core implementation for JSONB array element extraction operations. It validates that the input JSONB is an array (not a scalar or object), then iterates through each array element using a JsonbIterator. For each element, it creates a tuple containing either the JSONB value directly or its text representation, depending on the  parameter. The function uses a temporary memory context for efficient memory management during tuple processing and implements proper set-returning function (SRF) protocols using PostgreSQL's materialized SRF framework.

## Parameters / Member Variables
- `fcinfo`: Function call information context containing arguments and result information
- `*funcname`: Name of the calling function (used for error reporting)
- `as_text`: Boolean flag determining output format - true returns text, false returns JSONB
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Extract JSONB argument from function call
  - JB_ROOT_IS_SCALAR/JB_ROOT_IS_ARRAY: JSONB type checking macros
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md): Initialize set-returning function framework
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)/JsonbIteratorNext: JSONB iteration functions
  - [JsonbValueAsText](../J/JsonbValueAsText.md)/JsonbValueToJsonb: JSONB value conversion functions
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md): Store result tuples
  - AllocSetContextCreate/MemoryContextDelete: Memory management
- Called from:
  - [jsonb_array_elements](../j/jsonb_array_elements.md): Main entry point for JSONB array element extraction
  - [jsonb_array_elements_text](../j/jsonb_array_elements_text.md): Main entry point for text-mode array element extraction

## Notes and Other Information
- Raises errors for scalar and object JSONB inputs, as only arrays are valid for element extraction
- Uses WJB_ELEM token to identify array elements during iteration
- Handles JSON null values appropriately in text mode by setting SQL null flags
- Implements efficient memory management with temporary contexts that are reset after each tuple
- Part of PostgreSQL's JSONB function suite for array processing operations

## Simplified Source

```c
static Datum
elements_worker_jsonb(FunctionCallInfo fcinfo, const char *funcname, bool as_text)
{
    Jsonb *jb = PG_GETARG_JSONB_P(0);
    ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;

    // Validate input is an array
    if (JB_ROOT_IS_SCALAR(jb))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("cannot extract elements from a scalar")));
    if (!JB_ROOT_IS_ARRAY(jb))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("cannot extract elements from an object")));

    // Initialize set-returning function
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);

    // Create temporary memory context for processing
    MemoryContext tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                                  "jsonb_array_elements temporary cxt",
                                                  ALLOCSET_DEFAULT_SIZES);

    // Iterate through array elements
    JsonbIterator *it = JsonbIteratorInit(&jb->root);
    JsonbValue v;
    JsonbIteratorToken r;
    bool skipNested = false;

    while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE) {
        skipNested = true;

        if (r == WJB_ELEM) {
            Datum values[1];
            bool nulls[1] = {false};

            // Switch to temporary context for tuple processing
            MemoryContext old_cxt = MemoryContextSwitchTo(tmp_cxt);

            // Convert element to requested format
            if (as_text) {
                if (v.type == jbvNull) {
                    nulls[0] = true;
                    values[0] = (Datum) NULL;
                } else {
                    values[0] = PointerGetDatum(JsonbValueAsText(&v));
                }
            } else {
                // Return as JSONB
                Jsonb *val = JsonbValueToJsonb(&v);
                values[0] = PointerGetDatum(val);
            }

            // Store the tuple result
            tuplestore_putvalues(rsi->setResult, rsi->setDesc, values, nulls);

            // Clean up temporary context
            MemoryContextSwitchTo(old_cxt);
            MemoryContextReset(tmp_cxt);
        }
    }

    MemoryContextDelete(tmp_cxt);
    PG_RETURN_NULL();
}
```