# getIthJsonbValueFromContainer

## Location
src/backend/utils/adt/jsonb_util.c: 465 - 501

## Overview
Retrieves the element at a specified index position from a JSONB array container with bounds checking.

## Definition


## Detailed Description
This function provides indexed access to elements within a JSONB array container. It performs bounds checking to ensure the requested index is valid, then extracts the element at that position using the container's internal storage layout. The function calculates the appropriate base address for the array elements and uses the container's offset information to locate and extract the specific element.

Unlike object lookups which require key comparison, array access is direct using integer indices. The function validates that the container is indeed an array type and throws an error if not. For valid indices, it allocates a new JsonbValue and fills it with the element data.

## Parameters / Member Variables
- : The JSONB array container to access (must be an array type)
- : The zero-based index of the element to retrieve

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