# ADDINC128

## Location
[src/common/sha2.c:115-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sha2.c#L115-L131)

## Overview
A macro for incrementally adding a 64-bit unsigned integer to a 128-bit unsigned integer represented as a two-element array of 64-bit words.

## Definition

```c
#define ADDINC128(w,n)	{ \
	(w)[0] += (uint64)(n); \
	if ((w)[0] < (n)) { \
		(w)[1]++; \
	} \
}
```
## Detailed Description
ADDINC128 is a utility macro designed to perform addition on 128-bit integers in cryptographic operations, specifically for SHA-512 hashing. The macro takes a 128-bit integer represented as an array of two 64-bit words and adds a 64-bit value to it. It handles overflow by incrementing the high-order word when the low-order word overflows. This is essential for maintaining accurate bit counts in hash operations that may exceed 64-bit limits.

## Parameters / Member Variables
-  07:09:53 up 10 days, 23:35,  0 users,  load average: 0.48, 0.46, 0.49
USER     TTY      FROM             LOGIN@   IDLE   JCPU   PCPU WHAT: A two-element array of uint64 representing the 128-bit integer (w[0] is low-order, w[1] is high-order)
- : The 64-bit unsigned integer value to be added to the 128-bit integer

## Dependencies
- Functions called/Symbols referenced:
  - None (pure macro expansion)
- Called from (representative examples):
  - pg_sha512_update (multiple locations in src/common/sha2.c)

## Notes and Other Information
- This macro is specifically used in SHA-512 operations where bit counts can exceed 64-bit limits
- The overflow check  detects when addition to the low-order word has wrapped around
- Located in src/common/sha2.c as part of PostgreSQL's cryptographic hash implementations
- Essential for maintaining accurate message length tracking in SHA-512 computations