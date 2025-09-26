# pg_sha1_init

## Location
src/common/sha1.c: 316 - 331

## Overview
Initializes a SHA-1 context structure with the standard initial hash values and resets all state variables to prepare for a new hash computation.

## Definition

```c
void
pg_sha1_init(pg_sha1_ctx *ctx)
```
## Detailed Description
The  function prepares a SHA-1 context for a new hash computation by:

1. **Zero initialization**: Clears the entire context structure to ensure all fields start in a known state
2. **Hash value initialization**: Sets the five 32-bit hash state variables (H0-H4) to the SHA-1 standard initial values as specified in FIPS PUB 180-1

The initial hash values are the fractional parts of the square roots of the first five prime numbers (2, 3, 5, 7, 11), expressed as hexadecimal constants:
- H0 = 0x67452301
- H1 = 0xEFCDAB89  
- H2 = 0x98BADCFE
- H3 = 0x10325476
- H4 = 0xC3D2E1F0

This initialization ensures the SHA-1 algorithm starts from the correct initial state for producing standard-compliant hash digests.

## Parameters / Member Variables
- : Pointer to the SHA-1 context structure to initialize, which contains:
  - Hash state variables (H0-H4) 
  - Message buffer for incomplete blocks
  - Bit/byte counters for message length tracking
  - All fields are reset to proper initial values

## Dependencies
- Functions called/Symbols referenced:
  - : To zero-initialize the entire context structure
  - : Macro for accessing hash state variables in the context
  - : The SHA-1 context structure type

- Called from:
  - : Part of the generic cryptographic hash interface
  - Various applications requiring SHA-1 hash computation initialization

## Notes and Other Information
- This is a public function, part of PostgreSQL's external SHA-1 API
- Must be called before any  or  operations
- The context can be reused for multiple hash computations by calling this function again
- The initial values are mathematically significant and required for SHA-1 standard compliance
- Safe to call multiple times on the same context - fully reinitializes the state
- Forms part of the standard init/update/final pattern for hash computation