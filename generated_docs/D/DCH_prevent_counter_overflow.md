# DCH_prevent_counter_overflow

## Location
[src/backend/utils/adt/formatting.c:3962-3975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L3962-L3975)

## Overview
Prevents integer overflow in the DCH (Date/Time formatting Cache) counter by halving all age values when approaching the maximum integer value.

## Definition
```c
static inline void DCH_prevent_counter_overflow(void)
```

## Detailed Description
This function implements a critical safety mechanism for the DCH cache management system. The DCH cache uses a counter-based aging system where DCHCounter tracks the maximum age value among cache entries and increments on each access. To prevent integer overflow when DCHCounter approaches INT_MAX, this function halves all existing cache entry ages and the counter itself, preserving the relative ordering of entries while maintaining accurate tracking of which entries are oldest.

The function operates on the principle that relative age relationships are more important than absolute age values. By halving all ages simultaneously, the cache can continue operating indefinitely without losing the essential information needed for cache replacement decisions.

## Parameters / Member Variables
This function takes no parameters and operates on global cache state:
- Uses global `DCHCounter` variable to check for overflow condition
- Modifies global `DCHCache` array entries by halving their age values
- Uses global `n_DCHCache` to determine iteration bounds

## Dependencies
- Functions called/Symbols referenced:
  - DCHCounter (global variable)
  - DCHCache (global array)
  - n_DCHCache (global variable)
  - INT_MAX (standard constant)
- Called from:
  - [DCH_cache_getnew](DCH_cache_getnew.md)
  - [DCH_cache_search](DCH_cache_search.md)

## Notes and Other Information
- The function is declared as `static inline` for performance optimization since it is called frequently
- The overflow check uses `(INT_MAX - 1)` rather than `INT_MAX` to provide a safety margin
- The bit shift operation (`>>= 1`) efficiently divides by 2 while preserving integer values
- This mechanism ensures the cache can operate indefinitely without counter overflow
- The algorithm maintains cache effectiveness by preserving relative age relationships between entries

## Simplified Source

```c
static inline void
DCH_prevent_counter_overflow(void)
{
    if (DCHCounter >= (INT_MAX - 1))
    {
        // Halve all cache entry ages to prevent overflow
        for (int i = 0; i < n_DCHCache; i++)
            DCHCache[i]->age >>= 1;

        // Halve the counter itself
        DCHCounter >>= 1;
    }
}
```