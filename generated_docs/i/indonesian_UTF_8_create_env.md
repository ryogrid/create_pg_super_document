# indonesian_UTF_8_create_env

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:404-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c#L404-L405)

## Overview
Factory function that creates and initializes a Snowball environment structure specifically configured for Indonesian UTF-8 text stemming.

## Definition
```c
extern struct SN_env * indonesian_UTF_8_create_env(void)
```

## Detailed Description
This function serves as a wrapper around the generic Snowball environment creation function, providing the specific configuration parameters needed for Indonesian text processing. It allocates and initializes a SN_env structure with the appropriate settings for the Indonesian stemming algorithm.

The function is part of the public interface for the Indonesian Snowball stemmer, allowing external code to create a properly configured environment for processing Indonesian text.

## Parameters / Member Variables
- Returns: Pointer to a newly created and configured SN_env structure for Indonesian stemming

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md): Generic Snowball environment creation function, called with parameters (0, 2)
- Called from: This is an entry point function for creating Indonesian stemmer environments and is not called by other functions in the codebase

## Notes and Other Information
- The parameters passed to SN_create_env are (0, 2), where:
  - First parameter (0): Likely indicates the number of string variables needed
  - Second parameter (2): Indicates that 2 integer variables (I[0] and I[1]) are needed for the Indonesian algorithm
- This function must be called before using the Indonesian stemming functionality
- The returned environment should be freed using the corresponding close function when no longer needed
- Part of the standard Snowball stemmer interface pattern used across all language implementations

## Simplified Source

```c
extern struct SN_env * indonesian_UTF_8_create_env(void) {
    // Create Snowball environment with 0 string vars and 2 integer vars
    return SN_create_env(0, 2);
}
```