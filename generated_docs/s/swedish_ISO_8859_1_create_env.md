# swedish_ISO_8859_1_create_env

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_swedish.c:285-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_swedish.c#L285-L286)

## Overview
Creates a new Snowball stemmer environment specifically configured for Swedish text processing using the ISO-8859-1 character encoding.

## Definition
```c
extern struct SN_env * swedish_ISO_8859_1_create_env(void)
```

## Detailed Description
This function is a language-specific wrapper around the generic `SN_create_env` function, designed to initialize a Snowball stemmer environment for Swedish text processing with ISO-8859-1 encoding. It creates an environment with no string array (S_size = 0) and 2 integer slots (I_size = 2), which are the specific requirements for the Swedish stemming algorithm.

The function allocates memory for the stemmer environment structure and initializes all necessary components including the main string buffer and integer arrays needed for Swedish morphological analysis.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md)
- Called from (representative examples):
  - No direct callers found in the current codebase

## Notes and Other Information
- Part of the Snowball stemming library integrated into PostgreSQL for full-text search functionality
- The parameters (0, 2) passed to SN_create_env indicate:
  - 0 string arrays (S_size = 0)
  - 2 integer variables (I_size = 2)
- These parameters are specifically tuned for the Swedish stemming algorithm's requirements
- The function is declared as extern, making it available for external linkage
- Memory allocation failure in SN_create_env will result in NULL return value
- Should be paired with swedish_ISO_8859_1_close_env for proper cleanup