# dshash_strcpy

## Location
[src/backend/lib/dshash.c:622-637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L622-L637)

## Overview
A utility function that provides a wrapper around strcpy() specifically designed for use as a copy function in dynamic shared hash table operations.

## Definition

```c
void
dshash_strcpy(void *dest, const void *src, size_t size, void *arg)
```
## Detailed Description
dshash_strcpy is a copy function that forwards to the standard library strcpy() function. It serves as an adapter function that matches the signature expected by the dynamic shared hash table system for copying string data. The function includes an assertion to ensure that the source string length is less than the specified size to prevent buffer overflows.

## Parameters / Member Variables
- : Pointer to the destination buffer where the string will be copied
- : Pointer to the source string to be copied
- : The size of the destination buffer
- : Additional argument (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - strlen (from standard C library)
  - strcpy (from standard C library)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is designed to be used as a callback function in dynamic shared hash table operations where string copying is required
- The function includes safety checks via Assert() to prevent buffer overflows
- The  parameter is not used but is included to match the expected function signature for copy callbacks
- The function assumes null-terminated strings as it uses strlen() and strcpy()