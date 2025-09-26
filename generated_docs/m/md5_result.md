# md5_result

## Location
[src/common/md5.c:348-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5.c#L348-L381)

## Overview
Extracts the final MD5 hash digest from the completed MD5 context and formats it as a 16-byte binary output.

## Definition
```c
static void md5_result(uint8 *digest, pg_md5_ctx *ctx)
```

## Detailed Description
The `md5_result` function converts the internal MD5 state variables (four 32-bit words) into the final 128-bit (16-byte) MD5 hash digest. The MD5 algorithm maintains its state as four 32-bit words (md5_sta, md5_stb, md5_stc, md5_std), which are stored in the context as both individual words and as a byte array (md5_st8) through a union.

On little-endian systems, the function simply copies the 16 bytes directly from the context's byte array. On big-endian systems, it performs byte swapping for each 32-bit word to ensure the output conforms to the MD5 specification's little-endian byte ordering. This guarantees that the same input will produce identical hash values regardless of the system's endianness.

## Parameters / Member Variables
- `digest`: Pointer to a 16-byte buffer where the final MD5 hash will be stored
- `ctx`: Pointer to the completed MD5 context containing the final hash state

## Dependencies
- Functions called/Symbols referenced:
  - [pg_md5_ctx](../p/pg_md5_ctx.md) (MD5 context structure type)
  - memmove (standard library function for memory copying)
- Called from (representative examples):
  - [pg_md5_final](../p/pg_md5_final.md)

## Notes and Other Information
- This function is static and only used internally within the MD5 implementation
- Must be called only after the MD5 computation is complete (after padding and final processing)
- The output digest buffer must be at least 16 bytes in size
- Handles endianness conversion to ensure consistent MD5 output across different architectures
- The byte ordering in the final digest follows the MD5 specification (RFC 1321) little-endian format
- This function completes the MD5 hashing process by providing the final binary hash value