# hash_combine64

## Location
src/include/common/hashfn.h: 80 - 91

## Overview
The `hash_combine64` function efficiently combines two 64-bit hash values into a single 64-bit hash value, providing good bit mixing for applications requiring composite hash values.

## Definition
```c
static inline uint64 hash_combine64(uint64 a, uint64 b)
```

## Detailed Description
`hash_combine64` implements a sophisticated hash combination algorithm designed to merge two 64-bit hash values while maintaining good statistical properties and bit distribution. The function uses a carefully chosen random constant (0x49a0f4dd15e5a8e3) combined with bit shifting and XOR operations to ensure proper mixing of input bits.

The algorithm follows the same design principles as `hash_combine()` but operates on 64-bit values, making it suitable for extended hash scenarios where larger hash spaces are required. The implementation uses left and right bit shifts (54 and 7 positions respectively) along with XOR operations to achieve avalanche effects, where small changes in input produce significant changes in output.

## Parameters / Member Variables
- `a`: First 64-bit hash value to combine
- `b`: Second 64-bit hash value to combine

## Dependencies
- Functions called/Symbols referenced:
  - UINT64CONST (macro for 64-bit constants)
- Called from (representative examples):
  - [compute_partition_hash_value](../c/compute_partition_hash_value.md)
  - hash_resource_elem

## Notes and Other Information
This function is primarily used in specialized contexts within PostgreSQL, particularly in partitioning logic where hash values need to be combined for partition selection, and in resource management where composite hash keys are needed. The algorithm has been tested to produce good bit mixing properties, ensuring that the combined hash maintains the statistical qualities expected of a good hash function. The magic constant and shift values are carefully chosen to maximize avalanche effects and minimize bias in the output distribution.