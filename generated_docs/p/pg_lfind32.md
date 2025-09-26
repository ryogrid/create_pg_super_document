# pg_lfind32

## Location
[src/include/port/pg_lfind.h:153-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_lfind.h#L153-L209)

## Overview
The main entry point for optimized linear search of 32-bit integers, implementing a sophisticated multi-tier optimization strategy that automatically selects between SIMD and scalar approaches based on array size and platform capabilities.

## Definition
```c
static inline bool pg_lfind32(uint32 key, const uint32 *base, uint32 nelem)
```

## Detailed Description
The `pg_lfind32` function represents PostgreSQL's most advanced linear search implementation for 32-bit integers. It employs a sophisticated optimization strategy that automatically adapts to different scenarios:

1. **Small arrays**: Uses scalar one-by-one search via `pg_lfind32_one_by_one_helper`
2. **Large arrays**: Leverages SIMD operations through `pg_lfind32_simd_helper` processing 4 vector registers per iteration
3. **Overlap optimization**: Processes the final elements using overlapping SIMD operations to avoid scalar fallback

The function is conditionally compiled to fall back to scalar implementation when SIMD is not available (USE_NO_SIMD). When SIMD is available, it processes elements in blocks of 4 vector registers for maximum instruction-level parallelism, then handles any remaining elements by re-processing the end of the array with SIMD operations (allowing some overlap but maintaining performance).

## Parameters / Member Variables
- `key`: The 32-bit value to search for in the array
- `base`: Pointer to the array of 32-bit unsigned integers to search through (marked as const)
- `nelem`: Number of elements in the array

## Dependencies
- Functions called/Symbols referenced:
  - [vector32_broadcast](../v/vector32_broadcast.md) (replicates key value across vector for SIMD operations)
  - Vector32 (32-bit vector data type)
  - [pg_lfind32_one_by_one_helper](pg_lfind32_one_by_one_helper.md) (scalar fallback implementation)
  - [pg_lfind32_simd_helper](pg_lfind32_simd_helper.md) (vectorized 4-register block processing)
  - USE_NO_SIMD (compilation flag controlling SIMD availability)
- Called from (representative examples):
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md) (transaction processing)
  - [XidIsConcurrent](../X/XidIsConcurrent.md) (concurrency control)
  - [XidInMVCCSnapshot](../X/XidInMVCCSnapshot.md) (snapshot management)
  - Various test functions in test_lfind module

## Notes and Other Information
- The function is declared as `static inline` for maximum performance optimization
- Implements conditional compilation for SIMD vs scalar execution paths
- Uses advanced overlap optimization to avoid scalar processing of tail elements
- Calculates optimal iteration sizes based on vector register capacity
- Includes debug assertions when USE_ASSERT_CHECKING is enabled
- Critical component in PostgreSQL's transaction processing and snapshot management systems
- Automatically adapts to array size with different optimization strategies for small vs large arrays
- The overlap strategy in final processing prioritizes consistent SIMD performance over avoiding redundant checks