# elements_array_element_end

## Location
[src/backend/utils/adt/jsonfuncs.c:2370-2415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2370-L2415)

## Overview
This function serves as a JSON parser callback that handles the end of array elements when converting JSON arrays to PostgreSQL table rows. It processes each array element value and stores it as a tuple in a tuplestore for result set generation.

## Definition

```c
structure check */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on a non-array",
						_state->function_name)));
```
## Detailed Description
The  function is a callback function used during JSON parsing to handle the completion of array element processing. It operates as part of PostgreSQL's JSON element extraction functionality, specifically designed to convert JSON array elements into table rows. 

The function processes array elements at the top level only (lex_level == 1), ignoring nested structures. For each valid array element, it creates a heap tuple containing the element's value and stores it in a tuplestore. The function handles both null values and scalar values, with special processing for normalized results when required.

Memory management is carefully handled using temporary memory contexts to ensure proper cleanup after each tuple is processed.

## Parameters / Member Variables
- : Pointer to ElementsState structure containing parser state and configuration
- : Boolean flag indicating whether the current array element is null

## Dependencies
- Functions called/Symbols referenced:
  - [ElementsState](../E/ElementsState.md) (state structure)
  - JSON_SUCCESS (return value constant)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (text conversion)
  - [heap_form_tuple](../h/heap_form_tuple.md) (tuple creation)
  - [tuplestore_puttuple](../t/tuplestore_puttuple.md) (tuple storage)
  - [MemoryContextReset](../M/MemoryContextReset.md) (memory management)
  - JsonParseErrorType (return type)

- Called from (representative examples):
  - [elements_worker](elements_worker.md) (main processing function)
  - JsObjectFree (cleanup context)

## Notes and Other Information
- Only processes elements at the top level of JSON arrays (lex_level == 1)
- Supports null value handling when normalize_results is enabled
- Uses temporary memory context for efficient memory management
- Part of PostgreSQL's JSON table function infrastructure
- Returns JSON_SUCCESS on successful processing
- Critical for json_array_elements() and related functions

## Simplified Source

```c
static JsonParseErrorType
elements_array_element_end(void *state, bool isnull)
{
    ElementsState *_state = (ElementsState *) state;
    MemoryContext old_cxt;
    int len;
    text *val;
    HeapTuple tuple;
    Datum values[1];
    bool nulls[1] = {false};

    // Only process top-level array elements, skip nested structures
    if (_state->lex->lex_level != 1)
        return JSON_SUCCESS;

    // Switch to temporary memory context for tuple processing
    old_cxt = MemoryContextSwitchTo(_state->tmp_cxt);

    // Handle different value types
    if (isnull && _state->normalize_results) {
        // Null value in text normalization mode
        nulls[0] = true;
        values[0] = (Datum) NULL;
    } else if (_state->next_scalar) {
        // Use pre-processed normalized scalar value
        values[0] = CStringGetTextDatum(_state->normalized_scalar);
        _state->next_scalar = false;
    } else {
        // Extract raw JSON text from token positions
        len = _state->lex->prev_token_terminator - _state->result_start;
        val = cstring_to_text_with_len(_state->result_start, len);
        values[0] = PointerGetDatum(val);
    }

    // Create and store tuple for this array element
    tuple = heap_form_tuple(_state->ret_tdesc, values, nulls);
    tuplestore_puttuple(_state->tuple_store, tuple);

    // Clean up temporary context and switch back
    MemoryContextSwitchTo(old_cxt);
    MemoryContextReset(_state->tmp_cxt);

    return JSON_SUCCESS;
}
```