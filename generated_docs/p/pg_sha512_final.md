# pg_sha512_final

## Location
src/common/sha2.c: 905 - 933

## Overview
Finalizes SHA-512 hash computation by processing any remaining data, outputting the final hash digest, and securely clearing the context structure.

## Definition


## Detailed Description
The  function completes the SHA-512 hashing process by:

1. **Final Processing**: Calls  to process any remaining buffered data and apply final padding
2. **Byte Order Conversion**: On little-endian systems, converts the internal state from host byte order to big-endian format using  macro for proper hash output
3. **Digest Output**: Copies the final 64-byte (512-bit) hash value from the context's state array to the provided digest buffer
4. **Secure Cleanup**: Zeros out the entire context structure to prevent potential information leakage

The function handles both successful completion (when digest buffer is provided) and cleanup-only scenarios (when digest is NULL). This design allows for secure context destruction even when the final hash value is not needed.

## Parameters / Member Variables
- : Pointer to the SHA-512 context structure containing the current hash state and any buffered input data
- : Output buffer to receive the final 64-byte SHA-512 hash digest, or NULL if only cleanup is desired

## Dependencies
- Functions called/Symbols referenced:
  -  - Processes final data block and applies padding
  -  - Macro for 64-bit byte order reversal on little-endian systems
  -  - Copies hash state to output digest buffer
  -  - Securely clears the context structure
- Referenced types:
  -  - SHA-512 context structure type
  -  - Constant defining 64-byte digest length
- Called from (representative examples):
  -  - Generic cryptographic hash finalization wrapper

## Notes and Other Information
- The function always clears the context structure with , ensuring sensitive data cannot be recovered after finalization
- On little-endian architectures, byte order conversion is required because SHA-512 specification mandates big-endian output format
- The digest parameter can be NULL, allowing the function to be used solely for secure context cleanup
- This implementation follows RFC 6234 specifications for SHA-512 hash computation
- The function is part of PostgreSQL's internal cryptographic library and should not be called directly by user code