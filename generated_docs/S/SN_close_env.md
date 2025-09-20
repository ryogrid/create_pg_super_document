# SN_close_env

## Location
[src/backend/snowball/libstemmer/api.c:34-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/api.c#L34-L50)

## Overview
Properly deallocates and cleans up a Snowball stemming environment, freeing all associated memory resources.

## Definition

```c
}

extern void SN_close_env(struct SN_env * z, int S_size)
```
## Detailed Description
This function is the destructor counterpart to . It performs a complete cleanup of a Snowball stemming environment by systematically deallocating all memory resources that were allocated during environment creation. The function handles:

- Deallocation of all symbol structures in the S array using 
- Freeing the S array pointer itself
- Freeing the I (integer) array
- Deallocating the primary symbol buffer (p)
- Finally freeing the environment structure itself

The function is designed to be safe with NULL pointers and handles partial cleanup scenarios (useful for error recovery in ).

## Parameters / Member Variables
- : Pointer to the SN_env structure to be deallocated (can be NULL)
- : Number of symbol slots in the S array (must match the size used in SN_create_env)

## Dependencies
- Functions called/Symbols referenced:
  - [lose_s](../l/lose_s.md) (deallocates symbol structures)
  - free (standard memory deallocation)

- Called from (representative examples):
  - [SN_create_env](SN_create_env.md) (error cleanup path)
  - Language-specific environment cleanup functions across all Snowball stemmers
  - [basque_ISO_8859_1_close_env](../b/basque_ISO_8859_1_close_env.md)
  - [english_UTF_8_close_env](../e/english_UTF_8_close_env.md)
  - [german_UTF_8_close_env](../g/german_UTF_8_close_env.md)
  - (and 40+ other language-specific stemmer cleanup functions)

## Notes and Other Information
- Safe to call with NULL environment pointer - returns immediately
- The S_size parameter must exactly match the size used when creating the environment
- Should always be called to prevent memory leaks when a stemming environment is no longer needed
- Used by all Snowball stemmer implementations for proper resource cleanup
- Called automatically by SN_create_env if initialization fails partway through