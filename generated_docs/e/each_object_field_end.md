# each_object_field_end

## Location
[src/backend/utils/adt/jsonfuncs.c:2118-2165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2118-L2165)

## Overview
A callback function used during JSON parsing that handles the end of each object field, extracting field names and values to populate a tuple store for PostgreSQL's JSON object expansion functions.

## Definition

```c
structure check */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot deconstruct an array as an object")));
```
## Detailed Description
This function is a callback used by PostgreSQL's JSON parser to process the completion of each object field during JSON object expansion operations. It operates at the top level of JSON objects (lex_level == 1) and creates tuples containing field names and their corresponding values, storing them in a tuple store for later retrieval. The function handles both scalar values and complex JSON structures, with special processing for normalized results and null values.

The function switches to a temporary memory context for tuple creation, ensuring proper memory management and cleanup after each field is processed.

## Parameters / Member Variables
- : Pointer to an EachState structure containing parser state and configuration
- : C string containing the field name from the JSON object
- : Boolean indicating whether the field value is null

## Dependencies
- Functions called/Symbols referenced:
  - [EachState](../E/EachState.md) (state structure)
  - JSON_SUCCESS (return value constant)
  - CStringGetTextDatum (converts C string to PostgreSQL text datum)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (converts C string with specified length to text)
  - [heap_form_tuple](../h/heap_form_tuple.md) (creates a PostgreSQL heap tuple)
  - [tuplestore_puttuple](../t/tuplestore_puttuple.md) (stores tuple in tuple store)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (switches memory contexts)
  - [MemoryContextReset](../M/MemoryContextReset.md) (resets temporary memory context)
- Called from:
  - [each_worker](each_worker.md) (main JSON expansion worker function)
  - JsObjectFree (JSON object callback structure)

## Notes and Other Information
- Only processes fields at the top level of JSON objects (lex_level == 1), skipping nested objects
- Uses temporary memory context for tuple creation with automatic cleanup
- Supports normalized scalar results and handles null values appropriately
- Part of PostgreSQL's JSON expansion infrastructure for functions like json_each() and jsonb_each()
- Returns JSON_SUCCESS on successful completion

## Simplified Source

```c
static JsonParseErrorType
each_object_field_end(void *state, char *fname, bool isnull)
{
    EachState *each_state = (EachState *) state;

    // Only process top-level object fields
    if (each_state->lex->lex_level != 1)
        return JSON_SUCCESS;

    // Switch to temporary memory context for tuple creation
    MemoryContext old_cxt = MemoryContextSwitchTo(each_state->tmp_cxt);

    Datum values[2];
    bool nulls[2] = {false, false};

    // Set field name (key)
    values[0] = CStringGetTextDatum(fname);

    // Set field value based on type and normalization settings
    if (isnull && each_state->normalize_results)
    {
        // Handle null values in normalized mode
        nulls[1] = true;
        values[1] = (Datum) 0;
    }
    else if (each_state->next_scalar)
    {
        // Use pre-normalized scalar value
        values[1] = CStringGetTextDatum(each_state->normalized_scalar);
        each_state->next_scalar = false;
    }
    else
    {
        // Extract value from token range
        int len = each_state->lex->prev_token_terminator - each_state->result_start;
        text *val = cstring_to_text_with_len(each_state->result_start, len);
        values[1] = PointerGetDatum(val);
    }

    // Create and store the key-value tuple
    HeapTuple tuple = heap_form_tuple(each_state->ret_tdesc, values, nulls);
    tuplestore_puttuple(each_state->tuple_store, tuple);

    // Clean up temporary memory and restore context
    MemoryContextSwitchTo(old_cxt);
    MemoryContextReset(each_state->tmp_cxt);

    return JSON_SUCCESS;
}
```