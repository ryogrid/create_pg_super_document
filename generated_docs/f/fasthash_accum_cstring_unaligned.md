# fasthash_accum_cstring_unaligned

## Location
[src/include/common/hashfn_unstable.h:224-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/hashfn_unstable.h#L224-L250)

## Overview
Processes a null-terminated C string by accumulating it into the hash state in chunks, returning the total length processed.

## Definition
```c
static inline size_t
fasthash_accum_cstring_unaligned(fasthash_state *hs, const char *str)
```

## Detailed Description
The `fasthash_accum_cstring_unaligned` function serves as the core workhorse for hashing null-terminated C strings in the fasthash incremental interface. It processes the input string in chunks of up to 8 bytes (the size of the accumulator), automatically handling the null terminator and returning the total length of the string processed.

The function implements an efficient chunking algorithm:
1. Iterates through the string until the null terminator is found
2. For each iteration, determines the optimal chunk size (up to `FH_SIZEOF_ACCUM` bytes or until null terminator)
3. Calls `fasthash_accum()` to process each chunk into the hash state
4. Advances the string pointer and continues until the entire string is processed

This approach provides several benefits:
- Eliminates the need for a separate `strlen()` call, improving performance
- Processes strings of arbitrary length efficiently
- Handles the null terminator correctly by stopping before it
- Returns the string length as a side effect, which can be used by finalizer functions

The function is marked as "unaligned" because it can handle strings at any memory address, regardless of alignment requirements.

## Parameters / Member Variables
- `hs`: Pointer to a `fasthash_state` structure that will accumulate the string data into its hash
- `str`: Pointer to the null-terminated C string to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [fasthash_accum](fasthash_accum.md) (processes each chunk of string data)
  - [fasthash_state](fasthash_state.md) (structure type being operated on) 
  - `FH_SIZEOF_ACCUM` (constant defining maximum chunk size: sizeof(uint64))
- Called from (representative examples):
  - Functions marked with `pg_attribute_no_sanitize_address` (src/include/common/hashfn_unstable.h:279)
  - [fasthash_accum_cstring](fasthash_accum_cstring.md) (src/include/common/hashfn_unstable.h:299, 314)

## Notes and Other Information
- The function is declared as `static inline` for performance optimization since string hashing is a common operation
- Returns the total length of the string processed, which eliminates the need for a separate `strlen()` call
- The "unaligned" designation indicates it can handle strings at any memory alignment, providing maximum flexibility
- The chunking approach ensures optimal performance by processing data in accumulator-sized pieces
- This function is typically used by higher-level string hashing functions that need both the hash computation and length information
- The returned length value is often passed to finalizer functions like `fasthash_final32()` as a tweak parameter to improve hash quality
- Handles empty strings correctly by returning 0 without processing any data

## Simplified Source

```c
static inline size_t
fasthash_accum_cstring_unaligned(fasthash_state *hs, const char *str)
{
    const char *const start = str;

    // Process string in chunks until null terminator
    while (*str) {
        size_t chunk_len = 0;

        // Find chunk size up to accumulator size or null terminator
        while (chunk_len < FH_SIZEOF_ACCUM && str[chunk_len] != '\0')
            chunk_len++;

        // Process this chunk into the hash
        fasthash_accum(hs, str, chunk_len);
        str += chunk_len;
    }

    // Return total string length processed
    return str - start;
}
```