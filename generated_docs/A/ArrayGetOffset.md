# ArrayGetOffset

## Location
[src/backend/utils/adt/arrayutils.c:32-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayutils.c#L32-L56)

## Overview
Converts a multidimensional array subscript list into a linear element offset for array element access calculations.

## Definition

```c
int
ArrayGetOffset(int n, const int *dim, const int *lb, const int *indx)
```
## Detailed Description
ArrayGetOffset calculates the linear offset (0-based index) for accessing an element in a multidimensional array stored in row-major order. The function performs the mathematical conversion from n-dimensional subscripts to a single linear index by multiplying each dimension's offset by the cumulative size of all subsequent dimensions.

The algorithm iterates backwards through the dimensions, calculating the offset contribution of each dimension and accumulating the scale factor. This approach efficiently handles arbitrary numbers of dimensions while maintaining the correct row-major ordering expected by PostgreSQL's array storage format.

The function assumes that all input parameters have been validated by the caller - specifically that dimensions and subscripts are within valid ranges to prevent arithmetic overflow.

## Parameters / Member Variables
- `n`: Number of dimensions in the array
- `*dim`: Array of dimension sizes for each dimension
- `*lb`: Array of lower bound values for each dimension
- `*indx`: Array of subscript indices to convert to linear offset
## Dependencies
- Functions called/Symbols referenced:
  - (No external functions called)
- Called from (representative examples):
  - [array_get_element](../a/array_get_element.md)
  - [array_get_element_expanded](../a/array_get_element_expanded.md)
  - [array_set_element](../a/array_set_element.md)
  - [array_set_element_expanded](../a/array_set_element_expanded.md)
  - [array_slice_size](../a/array_slice_size.md)
  - [array_extract_slice](../a/array_extract_slice.md)
  - [array_insert_slice](../a/array_insert_slice.md)

## Notes and Other Information
- Assumes caller has performed range checking on dimensions and subscripts to prevent overflow
- Uses row-major ordering consistent with PostgreSQL's internal array representation
- The algorithm processes dimensions in reverse order for efficiency in the row-major layout
- Critical utility function for all array element access operations in PostgreSQL

## Simplified Source
```c
int
ArrayGetOffset(int n, const int *dim, const int *lb, const int *indx)
{
    int offset = 0;
    int scale = 1;

    // Process dimensions in reverse order (row-major)
    for (int i = n - 1; i >= 0; i--) {
        // Calculate offset for this dimension
        offset += (indx[i] - lb[i]) * scale;

        // Update scale for next dimension
        scale *= dim[i];
    }

    return offset;
}
```