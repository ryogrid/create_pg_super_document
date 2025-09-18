# fasthash_combine

## Location
[src/include/common/hashfn_unstable.h:128-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn_unstable.h#L128-L135)

## Overview
Combines one chunk of input data from the accumulator into the running hash state for incremental hashing operations.

## Definition
```c
static inline void
fasthash_combine(fasthash_state *hs)
```

## Detailed Description
The `fasthash_combine` function is a key component of the incremental fasthash interface. It takes input data that has been stored in the `accum` field of the hash state and combines it with the current hash value using the fasthash algorithm's mixing operations.

The function performs two main operations:
1. Mixes the accumulated input using `fasthash_mix()` with a tweak value of 0, then XORs the result with the current hash
2. Multiplies the hash by the fasthash algorithm's signature constant (0x880355f21e6d1965) to further distribute the bits

This two-step process ensures that each chunk of input thoroughly influences the final hash value while maintaining the algorithm's statistical properties. The function is designed to be called multiple times during incremental hashing, with each call processing the data currently stored in the `accum` field.

## Parameters / Member Variables
- `hs`: Pointer to a `fasthash_state` structure containing:
  - `accum`: The input data to be combined into the hash (read by this function)
  - `hash`: The current hash value that will be updated with the combined input

## Dependencies
- Functions called/Symbols referenced:
  - [fasthash_mix](fasthash_mix.md) (applies core mixing algorithm to the accumulated input)
  - [fasthash_state](fasthash_state.md) (structure type being operated on)
- Called from (representative examples):
  - [spcachekey_hash](../s/spcachekey_hash.md) (src/backend/catalog/namespace.c:262)
  - [fasthash_accum](fasthash_accum.md) (src/include/common/hashfn_unstable.h:210)
  - Functions marked with `pg_attribute_no_sanitize_address` (src/include/common/hashfn_unstable.h:274)

## Notes and Other Information
- This function is declared as `static inline` for performance optimization since it's called frequently in tight loops
- The constant 0x880355f21e6d1965 appears both here and in `fasthash_init()`, serving as a signature constant for the fasthash algorithm
- Users typically load data into `hs->accum` before calling this function, often using direct assignment for simple types or `fasthash_accum()` for more complex data
- This function should be called once for each chunk of input data during incremental hashing, with the final hash obtained through `fasthash_final32()` or `fasthash_final64()`
- The function modifies the hash state in place, so the same state structure accumulates the effects of all combined inputs