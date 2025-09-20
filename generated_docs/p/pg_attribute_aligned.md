# pg_attribute_aligned

## Location
[src/include/c.h:567-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/c.h#L567-L573)

## Overview
A preprocessor macro that provides a portable interface for specifying memory alignment requirements for variables and structures using GCC's aligned attribute.

## Definition

```c
typedef unsigned PG_INT128_TYPE uint128
#if defined(pg_attribute_aligned)
			pg_attribute_aligned(MAXIMUM_ALIGNOF)
#endif
		   ;
```
## Detailed Description
This macro wraps GCC's  functionality to ensure that variables or structures are aligned to specific byte boundaries in memory. Memory alignment is crucial for performance optimization and ensuring proper access patterns, especially for atomic operations and I/O operations. The macro provides a consistent interface across different compiler environments while abstracting the underlying compiler-specific syntax.

## Parameters / Member Variables
- : The alignment boundary in bytes (must be a power of 2)

## Dependencies
- Functions called/Symbols referenced:
  - None (preprocessor macro)
- Called from (representative examples):
  - PGIOAlignedBlock (used for I/O buffer alignment)
  - PGAlignedXLogBlock (used for WAL buffer alignment)
  - [pg_atomic_uint64](pg_atomic_uint64.md) (used for atomic variable alignment)
  - [ItemPointerData](../I/ItemPointerData.md) (used for tuple identifier alignment)

## Notes and Other Information
- This macro is essential for ensuring proper alignment of data structures that require specific memory boundaries
- Commonly used for I/O buffers, atomic variables, and performance-critical data structures
- The alignment value must be a power of 2 (1, 2, 4, 8, 16, etc.)
- Proper alignment can significantly improve performance by avoiding cache line splits and enabling vectorized operations
- Used extensively in PostgreSQL's storage layer and atomic operations implementation