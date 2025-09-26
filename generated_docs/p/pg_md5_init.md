# pg_md5_init

## Location
[src/common/md5.c:382-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5.c#L382-L399)

## Overview
Initializes an MD5 context structure with the standard MD5 initial values and resets all state variables to begin a new MD5 hash computation.

## Definition
```c
void pg_md5_init(pg_md5_ctx *ctx)
```

## Detailed Description
The `pg_md5_init` function prepares an MD5 context for a new hashing operation by setting all necessary initial values as specified in RFC 1321. It initializes the four MD5 state variables (md5_sta, md5_stb, md5_stc, md5_std) to their magic constant starting values (MD5_A0, MD5_B0, MD5_C0, MD5_D0), which are specifically chosen hexadecimal values that provide good hash distribution properties.

The function also resets the message length counter (md5_n) and buffer index (md5_i) to zero, and clears the internal 64-byte processing buffer. This ensures that the context is in a clean state, ready to process input data through subsequent calls to pg_md5_update() and eventually pg_md5_final().

## Parameters / Member Variables
- `ctx`: Pointer to the MD5 context structure that will be initialized for hash computation

## Dependencies
- Functions called/Symbols referenced:
  - [pg_md5_ctx](pg_md5_ctx.md) (MD5 context structure type)
  - MD5_A0 (initial value constant for state variable A: 0x67452301)
  - MD5_B0 (initial value constant for state variable B: 0xefcdab89)
  - MD5_C0 (initial value constant for state variable C: 0x98badcfe)
  - MD5_D0 (initial value constant for state variable D: 0x10325476)
  - memset (standard library function for memory initialization)
- Called from (representative examples):
  - [pg_cryptohash_init](pg_cryptohash_init.md)

## Notes and Other Information
- This is a public function (non-static) and part of PostgreSQL's external MD5 API
- Must be called before any pg_md5_update() calls to ensure proper context initialization
- The magic constants (MD5_A0, MD5_B0, MD5_C0, MD5_D0) are the standard MD5 initialization values from RFC 1321
- The context can be reused for multiple hash computations by calling pg_md5_init() again
- Thread-safe as it only modifies the provided context structure
- Essential first step in PostgreSQL's MD5 hashing workflow: init → update → final