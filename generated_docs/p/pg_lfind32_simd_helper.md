# pg_lfind32_simd_helper

## Location
[src/include/port/pg_lfind.h:109-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_lfind.h#L109-L152)

## Overview
A highly optimized SIMD implementation that searches for a key across four vector registers simultaneously, processing multiple 32-bit integers in parallel for maximum performance.

## Definition
```c
static inline bool pg_lfind32_simd_helper(const Vector32 keys, const uint32 *base)
```

## Detailed Description
The `pg_lfind32_simd_helper` function implements an advanced vectorized search algorithm that processes four full vector registers worth of 32-bit integers simultaneously. This function represents the high-performance core of PostgreSQL's 32-bit linear search implementation, leveraging SIMD instructions to achieve maximum throughput.

The function loads data into four separate vector registers, performs parallel equality comparisons against the search key across all four vectors, then combines the results using logical OR operations. This approach maximizes CPU pipeline utilization and memory bandwidth while minimizing the number of iterations required.

## Parameters / Member Variables
- `keys`: A Vector32 containing the replicated search key values for SIMD comparison
- `base`: Pointer to the array of 32-bit integers to search (must have at least 4 vector registers worth of valid data)

## Dependencies
- Functions called/Symbols referenced:
  - Vector32 (32-bit vector data type for SIMD operations)
  - [vector32_load](../v/vector32_load.md) (loads 32-bit data into vector registers)
  - [vector32_eq](../v/vector32_eq.md) (performs vectorized equality comparison)
  - [vector32_or](../v/vector32_or.md) (performs vectorized logical OR operation)
  - [vector32_is_highbit_set](../v/vector32_is_highbit_set.md) (checks if any comparison result indicates a match)
- Called from (representative examples):
  - [pg_lfind32](pg_lfind32.md) (main 32-bit search function)

## Notes and Other Information
- The function is declared as `static inline` for maximum performance optimization
- Processes exactly 4 vector registers worth of data per call for optimal SIMD utilization
- The caller must ensure sufficient data availability (at least 4 vector registers worth)
- Uses advanced SIMD techniques including parallel loading, comparison, and result aggregation
- Part of PostgreSQL's sophisticated multi-tier optimization strategy for linear search
- Calculates nelem_per_vector dynamically based on vector and element sizes
- Combines results efficiently using a tree-like OR reduction pattern
- Returns true if any of the processed elements match the search key

## Simplified Source

```c
static inline bool
pg_lfind32_simd_helper(const Vector32 keys, const uint32 *base)
{
    const uint32 nelem_per_vector = sizeof(Vector32) / sizeof(uint32);
    Vector32 vals1, vals2, vals3, vals4;
    Vector32 result1, result2, result3, result4;
    Vector32 tmp1, tmp2, result;

    // Load 4 vector registers worth of data from array
    vector32_load(&vals1, base);
    vector32_load(&vals2, &base[nelem_per_vector]);
    vector32_load(&vals3, &base[nelem_per_vector * 2]);
    vector32_load(&vals4, &base[nelem_per_vector * 3]);

    // Compare search key against all loaded values in parallel
    result1 = vector32_eq(keys, vals1);
    result2 = vector32_eq(keys, vals2);
    result3 = vector32_eq(keys, vals3);
    result4 = vector32_eq(keys, vals4);

    // Combine all comparison results using OR operations
    tmp1 = vector32_or(result1, result2);
    tmp2 = vector32_or(result3, result4);
    result = vector32_or(tmp1, tmp2);

    // Return true if any comparison found a match
    return vector32_is_highbit_set(result);
}
```