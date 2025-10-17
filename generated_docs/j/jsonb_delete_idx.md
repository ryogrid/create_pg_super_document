# jsonb_delete_idx

## Location
[src/backend/utils/adt/jsonfuncs.c:4780-4843](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4780-L4843)

## Overview
Deletes an element from a JSONB array by its index, supporting both positive and negative indices.

## Definition

```c
Datum
jsonb_delete_idx(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a SQL-callable function that removes an element at a specified index from a JSONB array. It accepts positive indices (counting from the beginning) and negative indices (counting backward from the end). The function validates that the input is an array (not a scalar or object), handles edge cases like out-of-bounds indices gracefully, and returns a new JSONB array with the specified element removed.

The function creates a new JSONB value rather than modifying the input in place, following PostgreSQL's immutable data structure approach. It uses the JSONB iterator mechanism to traverse the array elements and rebuilds the array while skipping the target element.

## Parameters / Member Variables
- : The input JSONB array from which to delete an element
- : The index of the element to delete. Negative values count from the end

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Extract JSONB argument from function call
  - PG_GETARG_INT32: Extract integer argument from function call
  - JB_ROOT_IS_SCALAR: Check if JSONB root is a scalar value
  - JB_ROOT_IS_OBJECT: Check if JSONB root is an object
  - JB_ROOT_COUNT: Get the count of elements in JSONB root
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md): Initialize JSONB iterator
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md): Get next element from JSONB iterator
  - [pushJsonbValue](../p/pushJsonbValue.md): Add value to JSONB parse state
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md): Convert JsonbValue to Jsonb
  - PG_RETURN_JSONB_P: Return JSONB value from function
- Called from (representative examples):
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- Only works with JSONB arrays; returns error for scalars and objects
- Negative indices are supported (e.g., -1 for last element)
- Out-of-bounds indices return the original array unchanged
- Empty arrays return the original array unchanged
- Uses PostgreSQL's iterator pattern for efficient JSONB traversal
- File location: src/backend/utils/adt/jsonfuncs.c:4780-4843

## Simplified Source

```c
Datum jsonb_delete_idx(PG_FUNCTION_ARGS) {
    Jsonb *in = PG_GETARG_JSONB_P(0);
    int idx = PG_GETARG_INT32(1);
    JsonbParseState *state = NULL;
    JsonbIterator *it;
    uint32 i = 0, n;
    JsonbValue v, *res = NULL;
    JsonbIteratorToken r;

    // Error checks: only arrays supported
    if (JB_ROOT_IS_SCALAR(in))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("cannot delete from scalar")));

    if (JB_ROOT_IS_OBJECT(in))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("cannot delete from object using integer index")));

    if (JB_ROOT_COUNT(in) == 0)
        PG_RETURN_JSONB_P(in);

    it = JsonbIteratorInit(&in->root);

    // Get array information
    r = JsonbIteratorNext(&it, &v, false);
    n = v.val.array.nElems;

    // Handle negative indices (count from end)
    if (idx < 0) {
        if (-idx > n)
            idx = n;  // Clamp to valid range
        else
            idx = n + idx;  // Convert to positive index
    }

    // Return unchanged if index out of bounds
    if (idx >= n)
        PG_RETURN_JSONB_P(in);

    // Start building result array
    pushJsonbValue(&state, r, NULL);

    // Iterate through array elements, skip target index
    while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE) {
        if (r == WJB_ELEM) {
            if (i++ == idx)
                continue;  // Skip element at target index
        }

        res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
    }

    PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}
```