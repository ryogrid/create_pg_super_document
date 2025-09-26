# array_iter_next

## Location
src/include/utils/arrayaccess.h: 81 - 118

## Overview
Retrieves the next element from a PostgreSQL array during sequential iteration, handling both expanded and flat array storage formats while managing null values and element alignment.

## Definition
```c
static inline Datum
array_iter_next(array_iter *it, bool *isnull, int i,
                int elmlen, bool elmbyval, char elmalign)
```

## Detailed Description
The `array_iter_next` function fetches elements sequentially from a PostgreSQL array using a previously initialized `array_iter` structure. It handles two different access patterns based on the array storage format:

For expanded arrays (when `datumptr` is not NULL), it directly accesses the Datum array and corresponding null flags. For flat arrays, it performs more complex operations:
- Checks the null bitmap using the current bitmask to determine if the element is NULL
- If not NULL, uses `fetch_att` to extract the element value from the binary data
- Advances the data pointer using `att_addlength_pointer` to account for variable-length data
- Aligns the pointer using `att_align_nominal` for proper memory alignment
- Updates the bitmask for the next null bitmap bit, wrapping to the next byte when necessary

The function must be called with elements in sequential order (index 0, 1, 2, etc.) as it maintains internal state for flat array traversal.

## Parameters / Member Variables
- `it`: Pointer to the initialized array_iter structure
- `isnull`: Output parameter set to true if the element is NULL, false otherwise
- `i`: Zero-based index of the element to retrieve (must be sequential)
- `elmlen`: Length of each array element (-1 for variable-length types)
- `elmbyval`: Whether elements are passed by value (true) or by reference (false)
- `elmalign`: Alignment requirement for array elements (c, s, i, or d)

## Dependencies
- Functions called/Symbols referenced:
  - fetch_att
  - att_addlength_pointer
  - att_align_nominal
- Called from (representative examples):
  - array_out
  - array_send
  - array_map
  - array_eq
  - array_cmp
  - hash_array
  - hash_array_extended
  - array_contain_compare
  - array_unnest_fctx

## Notes and Other Information
- This is an inline function defined in arrayaccess.h for performance
- Elements must be accessed sequentially; random access is not supported
- The bitmask starts at 1 and shifts left for each element, wrapping every 8 elements (0x100)
- For expanded arrays without null information (isnullptr is NULL), all elements are assumed non-NULL
- The function automatically handles memory alignment requirements for different data types
- Variable-length elements require proper length calculation using att_addlength_pointer
- The caller is responsible for providing the correct element index and type information