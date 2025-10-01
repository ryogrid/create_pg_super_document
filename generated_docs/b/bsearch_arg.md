# bsearch_arg

## Location
[src/port/bsearch_arg.c:55-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/bsearch_arg.c#L55-L78)

## Overview
A variant of the standard binary search algorithm that accepts an additional user-supplied argument to pass to the comparison function, enabling more flexible comparison operations.

## Definition
```c
void *bsearch_arg(const void *key, const void *base0,
                  size_t nmemb, size_t size,
                  int (*compar) (const void *, const void *, void *),
                  void *arg)
```

## Detailed Description
`bsearch_arg` performs a binary search on a sorted array, similar to the standard C library `bsearch()` function, but with the key difference that it passes an additional user-supplied argument to the comparison function. This allows for more complex comparison operations that may need access to external data or context.

The function uses an optimized binary search algorithm with bit-shift operations for efficient halving of the search space. The implementation includes detailed logic for handling both odd and even array sizes when moving left or right during the search process.

The algorithm works by repeatedly dividing the search space in half:
- When moving left after a failed comparison, it simply halves the limit
- When moving right, it adjusts the base pointer and decrements the limit before halving
- The search continues until either a match is found or the search space is exhausted

## Parameters / Member Variables
- `key`: Pointer to the object being searched for
- `base0`: Pointer to the first element of the sorted array to search
- `nmemb`: Number of elements in the array
- `size`: Size in bytes of each element in the array
- `compar`: Pointer to a comparison function that takes three arguments: the key, an array element, and the user-supplied argument
- `arg`: Additional user-supplied argument passed to the comparison function

## Dependencies
- Functions called/Symbols referenced: (none - uses only standard operations)
- Called from (representative examples):
  - [AssertCheckRanges](../A/AssertCheckRanges.md) (src/backend/access/brin/brin_minmax_multi.c:413)
  - [range_contains_value](../r/range_contains_value.md) (src/backend/access/brin/brin_minmax_multi.c:1085)
  - [statext_mcv_build](../s/statext_mcv_build.md) (src/backend/statistics/mcv.c:324)
  - [statext_mcv_serialize](../s/statext_mcv_serialize.md) (src/backend/statistics/mcv.c:956)

## Notes and Other Information
- Returns a pointer to the matching element if found, or NULL if not found
- The array must be sorted in ascending order according to the comparison function
- The comparison function should return negative, zero, or positive values for less-than, equal-to, or greater-than comparisons respectively
- This function is part of PostgreSQL's portability layer, providing consistent behavior across different platforms
- The implementation is based on the BSD bsearch algorithm with modifications to support the additional argument parameter
- Used primarily in PostgreSQL for searches that require context-sensitive comparisons, such as in statistics collection and BRIN index operations

## Simplified Source

```c
void *bsearch_arg(const void *key, const void *base0,
                  size_t nmemb, size_t size,
                  int (*compar)(const void *, const void *, void *),
                  void *arg) {
    const char *base = (const char *) base0;
    size_t lim;
    int cmp;
    const void *p;

    // Binary search loop - halve search space each iteration
    for (lim = nmemb; lim != 0; lim >>= 1) {
        // Calculate middle element
        p = base + (lim >> 1) * size;

        // Compare key with middle element
        cmp = (*compar)(key, p, arg);

        if (cmp == 0)
            return (void *) p;  // Found exact match

        if (cmp > 0) {
            // key > p: search right half
            base = (const char *) p + size;
            lim--;
        }
        // Otherwise search left half (lim already halved by >>= 1)
    }

    return NULL;  // Not found
}
```