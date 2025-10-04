# intset_binsrch_uint64

## Location
[src/backend/lib/integerset.c:714-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L714-L746)

## Overview
A binary search function for sorted arrays of 64-bit unsigned integers, providing efficient lookup and insertion point determination within IntegerSet operations.

## Definition

```c
static int
intset_binsrch_uint64(uint64 item, uint64 *arr, int arr_elems, bool nextkey)
```
## Detailed Description
This is a specialized binary search implementation designed for IntegerSet's internal operations. It searches through a sorted array of uint64 values and returns the position where a given key should be inserted to maintain sort order. The function supports two search modes controlled by the  parameter:

1. When  is false: Returns the position of an equal key if found, or the insertion point if not found
2. When  is true: Returns the position immediately after an equal key if found, or the insertion point if not found

The implementation uses the standard binary search algorithm with low and high pointers that converge on the target position. It's optimized to avoid integer overflow by using  instead of .

## Parameters / Member Variables
- `item`: The uint64 value to search for
- `*arr`: Pointer to the sorted array of uint64 values to search in
- `arr_elems`: Number of elements in the array
- `nextkey`: Boolean flag controlling behavior when equal keys are found
## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a self-contained utility function)
- Called from (representative examples):
  -  operations: Used internally during set construction and management
  - : Used to check membership of values in buffered arrays

## Notes and Other Information
- This is a static function, meaning it's only accessible within the integerset.c file
- Implements a lower-bound binary search algorithm
- The  parameter makes it versatile for different use cases within IntegerSet operations
- Returns an index that is always valid for insertion (0 to arr_elems inclusive)
- Uses overflow-safe midpoint calculation
- Time complexity is O(log n) where n is the number of array elements

## Simplified Source

```c
static int
intset_binsrch_uint64(uint64 item, uint64 *arr, int arr_elems, bool nextkey)
{
    int low, high, mid;

    low = 0;
    high = arr_elems;

    while (high > low)
    {
        mid = low + (high - low) / 2;  // Overflow-safe midpoint

        if (nextkey)
        {
            // Find position after equal key (or insertion point)
            if (item >= arr[mid])
                low = mid + 1;
            else
                high = mid;
        }
        else
        {
            // Find position of equal key (or insertion point)
            if (item > arr[mid])
                low = mid + 1;
            else
                high = mid;
        }
    }

    return low;
}
```