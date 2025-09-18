# portuguese_ISO_8859_1_create_env

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_portuguese.c: 958 - 959

## Overview
A factory function that creates and initializes a new Snowball environment structure specifically configured for Portuguese stemming with ISO-8859-1 character encoding.

## Definition
```c
extern struct SN_env * portuguese_ISO_8859_1_create_env(void)
```

## Detailed Description
This function serves as a language-specific wrapper around the generic SN_create_env function, providing the correct initialization parameters for Portuguese stemming operations. It allocates and initializes a SN_env structure with:

- 0 string arrays (S_size = 0): Portuguese stemming doesn't require additional string storage arrays
- 3 integer variables (I_size = 3): Allocates space for 3 integer variables used to track region boundaries and other stemming state

The function delegates to SN_create_env which handles the actual memory allocation, including:
- Main SN_env structure allocation
- Primary string buffer initialization (z->p)
- Integer array allocation for the 3 required variables
- Error handling with proper cleanup on allocation failure

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - SN_create_env (line 958): Generic environment creation function that allocates memory and initializes the stemming environment structure
- Called from:
  - External stemming interface (not referenced within this codebase)

## Notes and Other Information
- Returns a pointer to the newly created SN_env structure, or NULL on allocation failure
- The returned environment must be properly cleaned up using portuguese_ISO_8859_1_close_env
- The 3 integer variables (I_size = 3) are typically used for storing R1, R2, and RV region boundaries in Portuguese morphological analysis
- Part of the standard Snowball stemmer API pattern where each language provides create/close functions
- Memory allocation uses calloc to ensure zero-initialization of all fields