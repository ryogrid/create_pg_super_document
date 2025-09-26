# pg_sha1_final

## Location
src/common/sha1.c: 365 - 369

## Overview
Finalizes a SHA1 cryptographic hash computation by applying padding and extracting the final 160-bit hash digest from the context.

## Definition


## Detailed Description
The  function completes the SHA1 hash computation process by performing the final steps required by the SHA1 algorithm specification. It acts as a wrapper that orchestrates the finalization sequence:

1. **Padding Phase**: Calls  to apply the required SHA1 padding to the message. This includes appending a '1' bit followed by zero bits and the original message length as a 64-bit big-endian integer, ensuring the total padded message length is congruent to 448 modulo 512 bits.

2. **Result Extraction**: Calls  to extract the final 160-bit (20-byte) hash digest from the context's hash state and copy it to the destination buffer with proper endianness handling.

This function is part of PostgreSQL's internal SHA1 implementation, providing a clean interface for completing hash computations. After calling this function, the context should not be reused without reinitialization.

## Parameters / Member Variables
- : Pointer to the SHA1 context structure () that contains the current hash state, message buffer, bit count, and internal working variables. This context must have been previously initialized with  and optionally updated with .
- : Pointer to a buffer where the final 160-bit (20-byte) SHA1 hash digest will be written. The caller must ensure this buffer has at least 20 bytes of available space.

## Dependencies
- Functions called/Symbols referenced:
  - : Applies SHA1 padding to complete message processing
  - : Extracts and formats the final hash digest
  - : Context structure type containing hash state
- Called from (representative examples):
  - : Generic cryptographic hash finalization interface

## Notes and Other Information
- This function is part of PostgreSQL's internal cryptographic hash implementation located in 
- The function follows the SHA1 algorithm specification (RFC 3174) for proper message finalization
- After calling this function, the context is in a finalized state and should not be used for further updates without reinitialization
- The output digest is exactly 20 bytes (160 bits) as mandated by the SHA1 specification
- This implementation handles both big-endian and little-endian architectures through conditional compilation
- The function is thread-safe as it only operates on the provided context and output buffer without accessing global state