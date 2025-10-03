# pg_md5_final

## Location
[src/common/md5.c:432-436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5.c#L432-L436)

## Overview
Finalizes an MD5 hash computation by padding the remaining data and producing the final 16-byte MD5 digest.

## Definition

```c
void
pg_md5_final(pg_md5_ctx *ctx, uint8 *dest)
```
## Detailed Description
The  function completes the MD5 hash computation process by performing the final steps required by the MD5 algorithm. It takes an MD5 context that has been initialized with  and potentially updated with data using , then finalizes it to produce the complete 128-bit (16-byte) MD5 hash digest.

The function performs two critical operations:
1. **Padding**: Calls  to apply the MD5 padding scheme, which appends a '1' bit followed by zeros, and then the original message length as a 64-bit value. This ensures the message length is congruent to 448 bits modulo 512.
2. **Result extraction**: Calls  to extract the final hash value from the context's internal state and store it in the destination buffer.

This function is part of PostgreSQL's internal MD5 implementation and follows the standard MD5 algorithm specification (RFC 1321).

## Parameters / Member Variables
- `*ctx`: Pointer to the MD5 context structure () containing the current hash state. The context must have been previously initialized and may contain accumulated hash state from previous  calls.
- `*dest`: Pointer to a buffer where the final 16-byte MD5 digest will be stored. The caller must ensure this buffer is at least 16 bytes in size.
## Dependencies
- Functions called/Symbols referenced:
  - : Applies MD5 padding to complete the message block
  - : Extracts the final hash digest from the context state
  - : The MD5 context structure type
- Called from (representative examples):
  - : Generic cryptographic hash finalization wrapper

## Notes and Other Information
- This function modifies the MD5 context during finalization, so the context cannot be reused for further hashing without re-initialization
- The function does not perform any input validation; it assumes the context and destination pointer are valid
- The MD5 algorithm produces a fixed-size 128-bit (16-byte) output regardless of input size
- This is part of PostgreSQL's fallback MD5 implementation, used when system-provided crypto libraries are not available or preferred
- The function handles endianness concerns internally through the  function
- After calling this function, the dest buffer will contain the raw binary MD5 hash (not a hexadecimal string representation)

## Simplified Source

```c
void pg_md5_final(pg_md5_ctx *ctx, uint8 *dest)
{
    // Apply MD5 padding to complete the message
    md5_pad(ctx);

    // Extract the final hash digest from context
    md5_result(dest, ctx);
}
```