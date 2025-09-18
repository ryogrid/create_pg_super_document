# partition_hbound_cmp

## Location
src/backend/partitioning/partbounds.c: 3587 - 3606

## Overview
Compares hash partition bounds by comparing modulus first, then remainder if modulus values are equal.

## Definition


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