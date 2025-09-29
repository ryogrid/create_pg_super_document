# partition_hbound_cmp

## Location
[src/backend/partitioning/partbounds.c:3587-3606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3587-L3606)

## Overview
Compares hash partition bounds by comparing modulus first, then remainder if modulus values are equal.

## Definition

```c
static int32
partition_hbound_cmp(int modulus1, int remainder1, int modulus2, int remainder2)
```
## Detailed Description
This is a comparison function specifically designed for hash partition bounds. Hash partitioning in PostgreSQL uses a modulus-remainder approach where data is distributed across partitions based on hash(key) % modulus = remainder. The function implements a lexicographic comparison: it first compares the modulus values, and only if they are equal does it compare the remainder values. This ordering ensures consistent sorting of hash partition bounds during partition pruning and other operations.

## Parameters / Member Variables
- `modulus1`: The modulus value of the first hash partition bound
- `remainder1`: The remainder value of the first hash partition bound  
- `modulus2`: The modulus value of the second hash partition bound
- `remainder2`: The remainder value of the second hash partition bound

## Dependencies
- Functions called/Symbols referenced: None (pure comparison function)
- Called from:
  - compare_range_bounds (at src/backend/partitioning/partbounds.c:217)
  - [partition_hash_bsearch](partition_hash_bsearch.md) (at src/backend/partitioning/partbounds.c:3756)
  - [qsort_partition_hbound_cmp](../q/qsort_partition_hbound_cmp.md) (at src/backend/partitioning/partbounds.c:3783)

## Notes and Other Information
- Returns -1 if the first bound is less than the second, 1 if greater, and 0 if equal
- This is a static function internal to the partbounds.c module
- The comparison logic prioritizes modulus over remainder, which is essential for proper hash partition ordering
- Used in binary search operations and sorting of hash partition bounds

## Simplified Source

```c
static int32 partition_hbound_cmp(int modulus1, int remainder1, int modulus2, int remainder2) {
    // Compare modulus first
    if (modulus1 < modulus2)
        return -1;
    if (modulus1 > modulus2)
        return 1;

    // If modulus equal, compare remainder
    if (modulus1 == modulus2 && remainder1 != remainder2)
        return (remainder1 > remainder2) ? 1 : -1;

    // Everything equal
    return 0;
}
```