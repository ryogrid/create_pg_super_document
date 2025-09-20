# qunique_arg

## Location
[src/include/lib/qunique.h:46-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/qunique.h#L46-L67)

## Overview
A variant of qunique that removes duplicates from a pre-sorted array using a user-supplied comparator function that accepts an additional user data argument, providing compatibility with qsort_arg().

## Definition

```c
static inline size_t
qunique_arg(void *array, size_t elements, size_t width,
			int (*compare) (const void *, const void *, void *),
			void *arg)
```
## Detailed Description
The qunique_arg function extends the functionality of qunique by supporting comparator functions that require additional user data. This function removes duplicate elements from a pre-sorted array while passing a user-defined argument to the comparator function. Like qunique, it uses an in-place algorithm with a two-pointer technique to efficiently remove duplicates while maintaining the sorted order.

This variant is particularly useful when the comparison logic needs access to external data or configuration parameters. The function is designed to be compatible with arrays sorted using qsort_arg(), ensuring consistent behavior between sorting and duplicate removal operations.

The algorithm iterates through the array, comparing adjacent elements using the provided three-argument comparator function. When duplicates are detected (comparator returns 0), they are skipped. Unique elements are moved to the front of the array, and the function returns the new size of the deduplicated array.

## Parameters / Member Variables
- : Pointer to the array to be processed for duplicate removal
- : Number of elements in the input array
- : Size in bytes of each array element
- : Function pointer to three-argument comparator that returns 0 for equal elements, non-zero for different elements
- : User data argument passed through to the comparator function

## Dependencies
- Functions called/Symbols referenced:
  - [compare](../c/compare.md) (user-provided comparator function with extra argument)
  - memcpy (for moving array elements)
- Called from (representative examples):
  - [_bt_sort_array_elements](../b/_bt_sort_array_elements.md)

## Notes and Other Information
- The function is implemented as a static inline function for performance optimization
- The input array must be pre-sorted using qsort_arg() with the same comparator function and argument for correct operation
- Returns the new size of the array after duplicate removal
- The function modifies the array in-place, so the original array structure is altered
- For arrays with 0 or 1 elements, the function returns early without any processing
- The additional arg parameter allows for more complex comparison logic that depends on external state
- This is a generic utility that works with any data type through void pointers and element width specification
- The comparator function signature matches that used by qsort_arg() for consistency