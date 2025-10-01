# JsonbArraySize

## Location
[src/backend/utils/adt/jsonpath_exec.c:3225-3241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3225-L3241)

## Overview
Returns the size (number of elements) of a JSONB array, or -1 if the item is not an array.

## Definition
static int JsonbArraySize(JsonbValue *jb)

## Detailed Description
This utility function determines the size of a JSONB array by examining the JsonbValue structure. It specifically handles binary JSONB representations by checking if the container is an array type and not a scalar. The function includes an assertion that the input is not already of type jbvArray, suggesting it expects binary representations that need to be examined. This function is used during JSON path execution when array size information is needed for operations like indexing or iteration.

## Parameters / Member Variables
- jb: Pointer to the JsonbValue structure to examine for array size

## Dependencies
- Functions called/Symbols referenced:
  - JsonContainerIsArray (checks if container represents an array)
  - JsonContainerIsScalar (checks if container represents a scalar value)
  - JsonContainerSize (returns the number of elements in the container)
  - jbvArray (JsonbValue type constant for arrays)
  - jbvBinary (JsonbValue type constant for binary representations)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (JSON path execution function, multiple locations)

## Notes and Other Information
- This is a static utility function, only accessible within the jsonpath_exec.c module
- Contains an assertion that the input JsonbValue is NOT of type jbvArray
- Only works with binary JSONB representations (jbvBinary type)
- Returns -1 for non-array types or scalar arrays
- Part of the JSON path execution support infrastructure
- Used for array size determination during JSON path operations
- The assertion suggests this function expects preprocessed JSONB values in binary format

## Simplified Source

```c
static int
JsonbArraySize(JsonbValue *jb)
{
    // Function expects binary representation, not already-processed arrays
    Assert(jb->type != jbvArray);

    if (jb->type == jbvBinary) {
        JsonbContainer *container = jb->val.binary.data;

        // Check if it's an array (but not a scalar)
        if (JsonContainerIsArray(container) && !JsonContainerIsScalar(container))
            return JsonContainerSize(container);
    }

    // Not an array or invalid type
    return -1;
}
```