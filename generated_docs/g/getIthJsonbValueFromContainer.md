# getIthJsonbValueFromContainer

## Location
[src/backend/utils/adt/jsonb_util.c:465-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L465-L501)

## Overview
Retrieves the element at a specified index position from a JSONB array container with bounds checking.

## Definition

```c
JsonbValue *
getIthJsonbValueFromContainer(JsonbContainer *container, uint32 i)
```
## Detailed Description
This function provides indexed access to elements within a JSONB array container. It performs bounds checking to ensure the requested index is valid, then extracts the element at that position using the container's internal storage layout. The function calculates the appropriate base address for the array elements and uses the container's offset information to locate and extract the specific element.

Unlike object lookups which require key comparison, array access is direct using integer indices. The function validates that the container is indeed an array type and throws an error if not. For valid indices, it allocates a new JsonbValue and fills it with the element data.

## Parameters / Member Variables
- `*container`: The JSONB array container to access (must be an array type)
- `i`: The zero-based index of the element to retrieve
## Dependencies
- Functions called/Symbols referenced:
  - JsonContainerIsArray
  - JsonContainerSize
  - [fillJsonbValue](../f/fillJsonbValue.md)
  - [getJsonbOffset](getJsonbOffset.md)
- Called from (representative examples):
  - [jsonb_array_element](../j/jsonb_array_element.md)
  - [jsonb_array_element_text](../j/jsonb_array_element_text.md)
  - [jsonb_get_element](../j/jsonb_get_element.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)

## Notes and Other Information
- Strict type checking: will error (elog ERROR) if container is not an array
- Bounds checking: returns NULL for indices >= array size
- Always allocates a new JsonbValue for the result (caller responsible for freeing)
- Uses zero-based indexing like standard C arrays
- Direct O(1) access time for array elements
- Base address calculation accounts for the JEntry array structure preceding the actual element data

## Simplified Source

```c
JsonbValue *
getIthJsonbValueFromContainer(JsonbContainer *container, uint32 i)
{
    JsonbValue *result;
    char *base_addr;
    uint32 nelements;

    // Verify this is an array container
    if (!JsonContainerIsArray(container))
        elog(ERROR, "not a jsonb array");

    nelements = JsonContainerSize(container);

    // Bounds check - return NULL if index out of range
    if (i >= nelements)
        return NULL;

    // Calculate base address for element data
    base_addr = (char *) &container->children[nelements];

    // Allocate result and fill with element data
    result = palloc(sizeof(JsonbValue));
    fillJsonbValue(container, i, base_addr, getJsonbOffset(container, i), result);

    return result;
}
```