# pg_sha384_update

## Location
[src/common/sha2.c:944-949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L944-L949)

## Overview
Processes input data for SHA-384 hash computation by delegating to the SHA-512 update function, leveraging the shared algorithm structure between SHA-384 and SHA-512.

## Definition
```c
void pg_sha384_update(pg_sha384_ctx *context, const uint8 *data, size_t len)
```

## Detailed Description
The `pg_sha384_update` function is a thin wrapper that processes input data for SHA-384 hash computation. Since SHA-384 and SHA-512 use identical block processing algorithms and only differ in their initial hash values and final output length, this function simply casts the SHA-384 context to a SHA-512 context and calls `pg_sha512_update`.

This design approach:
1. **Code Reuse**: Eliminates code duplication by sharing the complex block processing logic between SHA-384 and SHA-512
2. **Type Safety**: Maintains distinct context types (`pg_sha384_ctx` vs `pg_sha512_ctx`) for API clarity while allowing safe casting
3. **Performance**: Avoids any overhead from reimplementing identical processing logic

The function can be called multiple times to process data incrementally, automatically handling partial blocks and maintaining state across calls.

## Parameters / Member Variables
- `context`: Pointer to the SHA-384 context structure containing current hash state and buffered data
- `data`: Pointer to input data bytes to be processed for hashing
- `len`: Number of bytes in the input data buffer to process

## Dependencies
- Functions called/Symbols referenced:
  - `[pg_sha512_update](pg_sha512_update.md)` - Core hash update function that handles all block processing logic
- Referenced types:
  - `[pg_sha384_ctx](pg_sha384_ctx.md)` - SHA-384 specific context structure type
  - `[pg_sha512_ctx](pg_sha512_ctx.md)` - SHA-512 context structure type (used for casting)
- Called from (representative examples):
  - `[pg_cryptohash_update](pg_cryptohash_update.md)` - Generic cryptographic hash update wrapper

## Notes and Other Information
- This function demonstrates an efficient implementation pattern where related algorithms share common processing code
- The context casting from `pg_sha384_ctx*` to `pg_sha512_ctx*` is safe because both context types have identical memory layouts
- Data can be processed in chunks of any size - the function handles buffering and block boundaries automatically
- Must be called after `pg_sha384_init` and before `pg_sha384_final` in the hash computation sequence
- The function follows the standard incremental hashing pattern allowing streaming of large datasets
- Part of PostgreSQL's internal cryptographic library and should not be called directly by user code

## Simplified Source

```c
void pg_sha384_update(pg_sha384_ctx *context, const uint8 *data, size_t len) {
    // SHA-384 shares the same algorithm as SHA-512, just different initial values
    // So we can safely cast and delegate to the SHA-512 implementation
    pg_sha512_update((pg_sha512_ctx *) context, data, len);
}
```