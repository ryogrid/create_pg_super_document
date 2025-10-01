# appendElement

## Location
[src/backend/utils/adt/jsonb_util.c:785-813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L785-L813)

## Overview
Appends an element to a JSONB array during construction, managing memory allocation and enforcing limits on the number of array elements.

## Definition
```c
static void appendElement(JsonbParseState *pstate, JsonbValue *scalarVal)
```

## Detailed Description
This function handles the addition of elements to JSONB arrays during the parsing and construction process. It performs several critical operations analogous to appendKey for objects:

1. **Validation**: Ensures that the current container is indeed a JSONB array
2. **Limit Enforcement**: Checks against JSONB_MAX_ELEMS to prevent excessive memory usage and maintain reasonable array sizes
3. **Memory Management**: Dynamically grows the elements array when needed, doubling the size each time to amortize allocation costs
4. **Element Storage**: Stores the element value and increments the array's element count

The function maintains the sequential order of elements as they are added, preserving the original array structure. It works with scalar values and nested containers, making it versatile for handling complex nested JSONB structures.

## Parameters / Member Variables
- `pstate`: Pointer to the current parse state containing the array being constructed
- `scalarVal`: Pointer to the JsonbValue containing the element to be added to the array

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md) (PostgreSQL memory reallocation)
  - ereport/errcode/errmsg (PostgreSQL error reporting)
  - JSONB_MAX_ELEMS (maximum allowed elements constant)
  - jbvArray (JSONB array type constant)
- Called from (representative examples):
  - [pushJsonbValueScalar](../p/pushJsonbValueScalar.md) (when processing WJB_ELEM tokens and nested arrays)

## Notes and Other Information
- This is a static function internal to jsonb_util.c, not exposed in the public API
- Uses assertions to validate input conditions and maintain invariants
- Implements exponential growth strategy for memory allocation (doubling size)
- Maintains insertion order of elements, crucial for array semantics
- Error reporting follows PostgreSQL conventions with specific error codes
- Memory management uses PostgreSQL's memory context system through repalloc
- Can handle both scalar values and nested containers (arrays and objects)
- Works as part of the sequential JSONB construction process

## Simplified Source

```c
static void
appendElement(JsonbParseState *pstate, JsonbValue *scalarVal)
{
    JsonbValue *array = &pstate->contVal;

    Assert(array->type == jbvArray);

    // Check for maximum elements limit
    if (array->val.array.nElems >= JSONB_MAX_ELEMS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("number of jsonb array elements exceeds the maximum allowed (%zu)",
                        JSONB_MAX_ELEMS)));

    // Grow array if needed (double size)
    if (array->val.array.nElems >= pstate->size) {
        pstate->size *= 2;
        array->val.array.elems = repalloc(array->val.array.elems,
                                          sizeof(JsonbValue) * pstate->size);
    }

    // Add element and increment count
    array->val.array.elems[array->val.array.nElems++] = *scalarVal;
}
```