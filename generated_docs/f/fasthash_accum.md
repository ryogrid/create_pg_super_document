# fasthash_accum

## Location
[src/include/common/hashfn_unstable.h:136-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn_unstable.h#L136-L216)

## Overview
Accumulates up to 8 bytes of input data into the hash state's accumulator and combines it into the running hash, handling endianness and partial byte sequences correctly.

## Definition
```c
static inline void
fasthash_accum(fasthash_state *hs, const char *k, size_t len)
```

## Detailed Description
The `fasthash_accum` function is a flexible input processing function for the incremental fasthash interface. It can handle variable-length input data up to 8 bytes (the size of the accumulator) and correctly loads this data into the hash state's `accum` field while respecting the platform's endianness.

The function uses a switch statement with fallthrough cases to efficiently handle different input lengths:
- For 8 bytes: Direct memory copy for optimal performance
- For 1-7 bytes: Carefully positioned bit shifts to place each byte in the correct position within the 64-bit accumulator
- For 0 bytes: Returns immediately without processing

The implementation includes separate code paths for big-endian and little-endian platforms to ensure consistent hash values regardless of the underlying architecture. After loading the input data into the accumulator, it calls `fasthash_combine()` to integrate the data into the running hash.

This function is particularly useful for processing variable-length data or when the input doesn't naturally align to 64-bit boundaries, providing more flexibility than the simpler interface of directly setting `hs->accum` and calling `fasthash_combine()`.

## Parameters / Member Variables
- `hs`: Pointer to a `fasthash_state` structure that will be updated with the input data
- `k`: Pointer to the input data buffer to be processed 
- `len`: Number of bytes to read from the input buffer, must be ≤ `FH_SIZEOF_ACCUM` (8 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [fasthash_combine](fasthash_combine.md) (combines the accumulated data into the hash)
  - [fasthash_state](fasthash_state.md) (structure type being operated on)
  - `FH_SIZEOF_ACCUM` (constant defining maximum accumulator size: sizeof(uint64))
  - `memcpy` (for efficient copying of aligned data)
  - `Assert` (validates input length constraint)
- Called from (representative examples):
  - [fasthash_accum_cstring_unaligned](fasthash_accum_cstring_unaligned.md) (src/include/common/hashfn_unstable.h:235)
  - [fasthash64](fasthash64.md) (src/include/common/hashfn_unstable.h:371, 376)

## Notes and Other Information
- The function is declared as `static inline` for performance optimization due to frequent usage in hashing operations
- The length parameter is validated with `Assert(len <= FH_SIZEOF_ACCUM)` to ensure buffer safety
- Endianness handling ensures consistent hash values across different architectures, which is critical for distributed systems
- The fallthrough switch statement design efficiently handles variable-length inputs without branching overhead
- This function is used internally by higher-level functions like `fasthash64()` to process input data in chunks
- For performance-critical code with known fixed-size inputs, direct accumulator assignment followed by `fasthash_combine()` may be preferred