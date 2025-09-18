# indonesian_ISO_8859_1_create_env

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c:404-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c#L404-L405)

## Overview
This function creates and initializes a Snowball stemming environment specifically configured for Indonesian language processing with ISO-8859-1 character encoding.

## Definition
```c
extern struct SN_env * indonesian_ISO_8859_1_create_env(void)
```

## Detailed Description
This is a factory function that creates a new Snowball environment (SN_env) configured for Indonesian stemming operations. It serves as the initialization entry point for the Indonesian stemmer, allocating and setting up the necessary data structures with language-specific parameters.

The function calls the generic SN_create_env with parameters tailored for Indonesian:
- 0 string variables 
- 2 integer variables (used for vowel counting and processing state during stemming)

This environment must be created before any Indonesian stemming operations can be performed and should be paired with indonesian_ISO_8859_1_close_env for proper cleanup.

## Parameters / Member Variables
- No parameters (void function)
- Returns: Pointer to newly created SN_env structure or NULL on allocation failure

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md) (generic Snowball environment creation function)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function allocates memory that must be freed using indonesian_ISO_8859_1_close_env
- The returned environment is ready for use with indonesian_ISO_8859_1_stem
- Part of the public API for the Indonesian Snowball stemmer
- The integer parameters (0, 2) specify the number of string and integer variables needed by the Indonesian stemming algorithm
- Follows the standard Snowball library pattern for language-specific environment creation