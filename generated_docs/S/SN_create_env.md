# SN_create_env

## Location
[src/backend/snowball/libstemmer/api.c:3-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/api.c#L3-L33)

## Overview
Creates and initializes a new Snowball stemming environment with specified array sizes for symbols and integers.

## Definition


## Detailed Description
This function is the primary constructor for the Snowball stemming library's environment structure (). It allocates memory for the environment and initializes its components based on the provided size parameters. The function creates a working environment that contains:

- A primary symbol buffer () for text processing
- An array of symbol pointers () for storing intermediate string results
- An array of integers () for storing numeric values during stemming operations

The function uses defensive programming practices with proper error handling - if any allocation fails, it cleans up previously allocated resources and returns NULL.

## Parameters / Member Variables
- : Number of symbol string slots to allocate in the S array (0 means no symbol array needed)
- : Number of integer slots to allocate in the I array (0 means no integer array needed)

## Dependencies
- Functions called/Symbols referenced:
  - calloc (memory allocation)
  - [create_s](../c/create_s.md) (creates symbol structures)
  - [SN_close_env](SN_close_env.md) (cleanup on error)
  - symbol (type reference)

- Called from (representative examples):
  - Language-specific environment creation functions across all Snowball stemmers
  - [basque_ISO_8859_1_create_env](../b/basque_ISO_8859_1_create_env.md)
  - [english_UTF_8_create_env](../e/english_UTF_8_create_env.md)
  - [german_UTF_8_create_env](../g/german_UTF_8_create_env.md)
  - (and 40+ other language-specific stemmer initialization functions)

## Notes and Other Information
- Returns NULL on any allocation failure
- Automatically calls SN_close_env for cleanup if initialization fails partway through
- Used by all Snowball stemmer implementations in PostgreSQL's full-text search
- The S_size and I_size parameters are language-specific and determined by the stemming algorithm requirements
- Memory allocated must be freed later using SN_close_env